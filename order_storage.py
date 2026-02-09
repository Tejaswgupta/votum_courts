import asyncio
import logging
import os
import re
import uuid
from datetime import datetime
from typing import Callable, Optional, Union, List, Dict
from urllib import parse

import requests

from supabase import Client, create_client

logger = logging.getLogger(__name__)

ORDER_STORAGE_BUCKET = os.getenv("ORDER_STORAGE_BUCKET", "documents")
SUPABASE_URL = os.getenv("SUPABASE_URL")
SUPABASE_KEY = os.getenv("SUPABASE_KEY")


def get_supabase_client() -> Optional[Client]:
    try:
        return create_client(SUPABASE_URL, SUPABASE_KEY)
    except Exception as exc:  # pragma: no cover - defensive logging only
        logger.warning("Failed to initialize Supabase client: %s", exc)
        return None


def _default_fetch(order_url: str, referer: Optional[str] = None) -> requests.Response:
    headers = {}
    if referer:
        headers["Referer"] = referer
    return requests.get(order_url, timeout=30, headers=headers)


def _format_order_timestamp(order_date: Optional[Union[str, datetime]]) -> str:
    """
    Format order date to dd-mm-yy for filenames.
    Falls back to current date if the source value is missing/unparseable.
    """
    if isinstance(order_date, datetime):
        return order_date.strftime("%d-%m-%y")

    if isinstance(order_date, str):
        cleaned = order_date.strip()
        if cleaned:
            known_formats = [
                "%d-%m-%Y",
                "%d-%m-%y",
                "%d/%m/%Y",
                "%d/%m/%y",
                "%Y-%m-%d",
                "%Y/%m/%d",
                "%d.%m.%Y",
                "%d %b %Y",
                "%d %B %Y",
            ]
            for fmt in known_formats:
                try:
                    return datetime.strptime(cleaned, fmt).strftime("%d-%m-%y")
                except ValueError:
                    continue

            match = re.search(r"(\d{1,2})[\/\-.](\d{1,2})[\/\-.](\d{2,4})", cleaned)
            if match:
                day, month, year = match.groups()
                year = f"20{year}" if len(year) == 2 else year
                try:
                    return datetime(
                        int(year),
                        int(month),
                        int(day),
                    ).strftime("%d-%m-%y")
                except ValueError:
                    pass

    return datetime.now().strftime("%d-%m-%y")


def _upload_order_document(
    order_url: Optional[str],
    order_index: int,
    case_id: Optional[str],
    order_date: Optional[Union[str, datetime]],
    supabase_client: Optional[Client],
    fetch_fn: Optional[Callable[[str, Optional[str]], requests.Response]] = None,
    referer: Optional[str] = None,
) -> Optional[dict]:
    """
    Download an order PDF and upload it to Supabase storage.
    Returns the public URL on success, otherwise None.
    """
    if not order_url or not supabase_client:
        return None

    if SUPABASE_URL and order_url.startswith(SUPABASE_URL):
        return {
            "public_url": order_url,
            "storage_bucket": ORDER_STORAGE_BUCKET,
            "storage_path": None,
            "source_url": order_url,
        }

    try:
        fetcher = fetch_fn or _default_fetch
        resp = fetcher(order_url, referer)
        content_type = (resp.headers.get("content-type") or "").lower()
        if "text/html" in content_type:
            logger.warning(
                "Expected PDF but got HTML from %s. Content preview: %s",
                order_url,
                resp.text[:200],
            )
            return None
        resp.raise_for_status()
    except Exception as exc:
        logger.warning("Failed to download order from %s: %s", order_url, exc)
        return None

    if not resp.content:
        return None

    # Use order passed date in dd-mm-yy format to avoid "today" timestamp drift.
    timestamp = _format_order_timestamp(order_date)
    storage_filename = f"order-{timestamp}.pdf"
    storage_path = f"case-{case_id}/Orders/{storage_filename}"

    try:
        supabase_client.storage.from_(ORDER_STORAGE_BUCKET).upload(
            storage_path,
            resp.content,
            {"content-type": resp.headers.get("content-type") or "application/pdf"},
        )
        public_url = supabase_client.storage.from_(ORDER_STORAGE_BUCKET).get_public_url(
            storage_path
        )
        return {
            "public_url": public_url,
            "storage_bucket": ORDER_STORAGE_BUCKET,
            "storage_path": storage_path,
            "source_url": order_url,
        }
    except Exception as exc:
        logger.warning("Failed to upload order to storage (%s): %s", storage_path, exc)
    return None


def _slugify_folder_name(value: str) -> str:
    slug = "".join(ch if ch.isalnum() else "-" for ch in value.lower().strip())
    slug = "-".join(filter(None, slug.split("-")))
    return slug or "folder"


async def persist_orders_to_storage(
    orders: Optional[List[dict]],
    case_id: Optional[str] = None,
    fetch_fn: Optional[Callable[[str, Optional[str]], requests.Response]] = None,
    base_url: Optional[str] = None,
    referer: Optional[str] = None,
) -> Optional[List[dict]]:
    """
    Upload scraped order documents to storage and replace document_url with the stored link.
    """
    if not orders:
        return orders

    supabase_client = get_supabase_client()
    processed_orders: list[dict] = []

    loop = asyncio.get_running_loop()
    tasks = []

    for idx, order in enumerate(orders):
        raw_url = (
            order.get("document_url")
            or order.get("source_document_url")
            or order.get("orderurlpath")
        )
        if raw_url and base_url and raw_url.startswith("/"):
            raw_url = parse.urljoin(base_url, raw_url)

        if supabase_client:
            task = loop.run_in_executor(
                None,
                _upload_order_document,
                raw_url,
                idx,
                case_id,
                order.get("date"),
                supabase_client,
                fetch_fn,
                referer,
            )
            tasks.append(task)
        else:
            future = loop.create_future()
            future.set_result(None)
            tasks.append(future)

    stored_results = await asyncio.gather(*tasks)

    for order, stored_result in zip(orders, stored_results):
        updated_order = dict(order)
        raw_url = (
            order.get("document_url")
            or order.get("source_document_url")
            or order.get("orderurlpath")
        )
        if raw_url and base_url and raw_url.startswith("/"):
            raw_url = parse.urljoin(base_url, raw_url)

        if stored_result and stored_result.get("public_url"):
            updated_order["source_document_url"] = raw_url
            updated_order["document_url"] = stored_result.get("public_url")

        processed_orders.append(updated_order)

    if supabase_client and case_id and processed_orders:
        workspace_id = None
        try:
            case_res = (
                supabase_client.table("votum_cases")
                .select("workspace_id")
                .eq("id", case_id)
                .limit(1)
                .execute()
            )
            if case_res.data:
                workspace_id = case_res.data[0].get("workspace_id")
        except Exception as exc:
            logger.warning("Failed to load case %s workspace: %s", case_id, exc)

        folder_id = None
        orders_folder_id = None
        if workspace_id:
            try:
                folder_res = (
                    supabase_client.table("document_folders")
                    .select("id")
                    .eq("workspace_id", workspace_id)
                    .eq("case_id", case_id)
                    .limit(1)
                    .execute()
                )
                if folder_res.data:
                    folder_id = folder_res.data[0].get("id")
            except Exception as exc:
                logger.warning(
                    "Failed to load document folder for case %s: %s",
                    case_id,
                    exc,
                )

        if workspace_id and folder_id:
            try:
                orders_slug = _slugify_folder_name("Orders")
                orders_res = (
                    supabase_client.table("document_folders")
                    .select("id")
                    .eq("workspace_id", workspace_id)
                    .eq("case_id", case_id)
                    .eq("parent_id", folder_id)
                    .eq("slug", orders_slug)
                    .limit(1)
                    .execute()
                )
                if orders_res.data:
                    orders_folder_id = orders_res.data[0].get("id")
                else:
                    payload = {
                        "name": "Orders",
                        "slug": orders_slug,
                        "workspace_id": workspace_id,
                        "parent_id": folder_id,
                        "case_id": case_id,
                        "created_by": None,
                    }
                    insert_res = (
                        supabase_client.table("document_folders")
                        .insert(payload)
                        .execute()
                    )
                    if insert_res.data:
                        if isinstance(insert_res.data, dict):
                            orders_folder_id = insert_res.data.get("id")
                        elif isinstance(insert_res.data, list) and insert_res.data:
                            orders_folder_id = insert_res.data[0].get("id")
                    if not orders_folder_id:
                        refresh_res = (
                            supabase_client.table("document_folders")
                            .select("id")
                            .eq("workspace_id", workspace_id)
                            .eq("case_id", case_id)
                            .eq("parent_id", folder_id)
                            .eq("slug", orders_slug)
                            .limit(1)
                            .execute()
                        )
                        if refresh_res.data:
                            orders_folder_id = refresh_res.data[0].get("id")
            except Exception as exc:
                logger.warning(
                    "Failed to ensure Orders folder for case %s: %s",
                    case_id,
                    exc,
                )

        for order, stored_result in zip(orders, stored_results):
            if not stored_result or not stored_result.get("public_url"):
                continue

            raw_url = (
                order.get("document_url")
                or order.get("source_document_url")
                or order.get("orderurlpath")
            )
            if raw_url and base_url and raw_url.startswith("/"):
                raw_url = parse.urljoin(base_url, raw_url)

            if not workspace_id:
                break

            try:
                existing = (
                    supabase_client.table("documents")
                    .select("id")
                    .eq("workspace_id", workspace_id)
                    .eq("case_id", case_id)
                    .contains("metadata", {"source_document_url": raw_url})
                    .limit(1)
                    .execute()
                )
                if existing.data:
                    continue
            except Exception:
                pass

            timestamp = _format_order_timestamp(order.get("date"))
            filename = f"order-{timestamp}.pdf"
            payload = {
                "workspace_id": workspace_id,
                "user_id": None,
                "pdf_url": stored_result.get("public_url"),
                "storage_bucket": stored_result.get("storage_bucket"),
                "storage_path": stored_result.get("storage_path"),
                "filename": filename,
                "tags": [],
                "annotations": [],
                "folder_id": orders_folder_id or folder_id,
                "document_type": "order",
                "status": "uploaded",
                "metadata": {
                    "source": "courts_order",
                    "source_document_url": raw_url,
                    "order_date": order.get("date"),
                    "order_description": order.get("description"),
                    "storage_bucket": stored_result.get("storage_bucket"),
                    "storage_path": stored_result.get("storage_path"),
                },
                "case_id": case_id,
            }

            try:
                supabase_client.table("documents").insert(payload).execute()
            except Exception as exc:
                logger.warning(
                    "Failed to insert order document for case %s: %s",
                    case_id,
                    exc,
                )

    if supabase_client and case_id and processed_orders != orders:
        try:
            await loop.run_in_executor(
                None,
                lambda: supabase_client.table("votum_cases").update(
                    {"orders": processed_orders}
                ).eq("id", case_id).execute(),
            )
        except Exception as exc:
            logger.warning("Failed to update orders for case %s: %s", case_id, exc)

    return processed_orders
