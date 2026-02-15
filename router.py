import logging
from typing import List, Optional

from fastapi import APIRouter, Body, HTTPException
from pydantic import BaseModel

logger = logging.getLogger(__name__)

from . import bombay_hc, gujarat_hc, hc_services
from .bombay_hc import get_bombay_case_details
from .bombay_hc import \
    persist_orders_to_storage as bombay_persist_orders_to_storage
from .dc_services import EcourtsWebScraper
from .dc_services import \
    persist_orders_to_storage as web_persist_orders_to_storage
from .delhi_hc import get_delhi_case_details
from .delhi_hc import \
    persist_orders_to_storage as delhi_persist_orders_to_storage
from .gujarat_hc import (get_gujarat_case_details,
                         get_gujarat_case_details_by_cnr_no,
                         get_gujarat_case_details_by_filing_no)
from .gujarat_hc import \
    persist_orders_to_storage as gujarat_persist_orders_to_storage
from .hc_services import hc_get_benches, hc_get_case_types, hc_get_states
from .NCLAT import (nclat_get_details, nclat_search_by_case_no,
                    nclat_search_by_free_text)
from .NCLAT import persist_orders_to_storage as nclat_persist_orders_to_storage
from .NCLT import (nclt_get_details, nclt_search_by_advocate_name,
                   nclt_search_by_case_number, nclt_search_by_filing_number,
                   nclt_search_by_party_name)
from .NCLT import persist_orders_to_storage as nclt_persist_orders_to_storage
from .SCI import (sci_get_details, sci_search_by_aor_code,
                  sci_search_by_case_number, sci_search_by_court,
                  sci_search_by_diary_number, sci_search_by_party_name)

router = APIRouter(prefix="/ecourts", tags=["ecourts"])



@router.get("/search_nclat_search_by_case_no/")
async def search_nclat_search_by_case_no(location: str, case_type: str, case_no: str, case_year: str):
    return nclat_search_by_case_no(location, case_type, case_no, case_year)


@router.get("/search_nclat_search_by_free_text/")
async def search_nclat_search_by_free_text(
    location: str, search_by: str, free_text: str, from_date: str, to_date: str
):
    return nclat_search_by_free_text(location, search_by, free_text, from_date, to_date)


@router.get("/search_nclt_search_by_filing_number/")
async def search_nclt_search_by_filing_number(bench: str, filing_number: str):
    return nclt_search_by_filing_number(bench, filing_number)


@router.get("/search_nclt_search_by_case_number/")
async def search_nclt_search_by_case_number(
    bench: str, case_type: str, case_number: str, case_year: str
):
    return nclt_search_by_case_number(bench, case_type, case_number, case_year)


@router.get("/search_nclt_search_by_party_name/")
async def search_nclt_search_by_party_name(
    bench: str, party_type: str, party_name: str, case_year: str, case_status: str
):
    return nclt_search_by_party_name(
        bench, party_type, party_name, case_year, case_status
    )


@router.get("/search_nclt_search_by_advocate_name/")
async def search_nclt_search_by_advocate_name(bench: str, advocate_name: str, year: str):
    return nclt_search_by_advocate_name(bench, advocate_name, year)


@router.get("/search_sci_search_by_diary_number/")
async def search_sci_search_by_diary_number(diary_number: str, diary_year: str):
    return sci_search_by_diary_number(diary_number, diary_year)


@router.get("/search_sci_search_by_case_number/")
async def search_sci_search_by_case_number(case_type: str, case_number: str, case_year: str):
    return sci_search_by_case_number(case_type, case_number, case_year)


@router.get("/search_sci_search_by_aor_code/")
async def search_sci_search_by_aor_code(party_type: str, aor_code: str, year: str, case_status: str):
    return sci_search_by_aor_code(party_type, aor_code, year, case_status)


@router.get("/search_sci_search_by_party_name/")
async def search_sci_search_by_party_name(
    party_type: str, party_name: str, year: str, party_status: str
):
    return sci_search_by_party_name(party_type, party_name, year, party_status)


@router.get("/search_sci_search_by_court/")
async def search_sci_search_by_court(
    court: str,
    state: str,
    bench: str,
    case_type: str,
    case_number: str,
    case_year: str,
    order_date: str,
):
    return sci_search_by_court(
        court, state, bench, case_type, case_number, case_year, order_date
    )


@router.get("/nclt_details/")
async def nclt_details(bench: str, filing_no: str):
    if not bench or not filing_no:
        return HTTPException(status_code=400, detail="bench and filing_no are required")
    return nclt_get_details(bench, filing_no)


@router.get("/sci_details/")
async def sci_details(diary_no: str, diary_year: str):
    return sci_get_details(diary_no, diary_year)


@router.get("/bombay_hc_details/", summary="Fetch Bombay High Court case details")
async def bombay_hc_details(case_type: str, case_no: str, case_year: str):
    return get_bombay_case_details(case_type, case_no, case_year)


@router.get("/gujarat_hc_details/", summary="Fetch Gujarat High Court case details")
async def gujarat_hc_details(case_type: str, case_no: str, case_year: str):
    return get_gujarat_case_details(case_type, case_no, case_year)


@router.get("/gujarat_hc_details_by_filing_no/", summary="Fetch Gujarat High Court case details by filing number")
async def gujarat_hc_details_by_filing_no(case_type: str, filing_no: str, filing_year: str):
    return get_gujarat_case_details_by_filing_no(case_type, filing_no, filing_year)


@router.get("/gujarat_hc_details_by_cnr_no/", summary="Fetch Gujarat High Court case details by CNR number")
async def gujarat_hc_details_by_cnr_no(cnr_no: str):
    return get_gujarat_case_details_by_cnr_no(cnr_no)




@router.get("/hc/search_by_case_number/", summary="Search High Court cases by case number")
async def hc_search_by_case_number(
    state_code: str,
    court_code: str,
    case_type: str,
    case_no: str,
    year: str,
):
    if state_code is not None and state_code == '15':
        return get_bombay_case_details(case_type, case_no, year)
    if state_code is not None and state_code == '17':
        return get_gujarat_case_details(case_type, case_no, year)
    if state_code is not None and state_code == '26':
        return get_delhi_case_details(case_type, case_no, year)
    return hc_services.hc_search_by_case_number(
        state_code=state_code,
        court_code=court_code,
        case_type=case_type,
        case_no=case_no,
        year=year,
    )


@router.get("/hc/search_by_party_name/", summary="Search High Court cases by party name")
async def hc_search_by_party_name(
    state_code: str,
    court_code: str,
    pet_name: str | None = None,
    res_name: str | None = None,
):
    return hc_services.hc_search_by_party_name(
        state_code=state_code,
        court_code=court_code,
        pet_name=pet_name,
        res_name=res_name,
    )


@router.get("/hc/search_by_cnr/", summary="Search High Court cases by CNR number")
async def hc_search_by_cnr(cnr_number: str):
    if cnr_number and cnr_number.startswith("GJHC"):
        try:
            res = get_gujarat_case_details_by_cnr_no(cnr_number)
            if res:
                return res
        except Exception as e:
            logger.warning(f"Direct Gujarat HC search failed for CNR {cnr_number}: {e}")
            
    return hc_services.hc_search_by_cnr(cnr_number)


@router.get("/hc/case_details/", summary="Get High Court case details")
async def hc_case_details(state_code: str, court_code: str, case_id: str):
    return hc_services.hc_get_case_details(
        state_code=state_code,
        court_code=court_code,
        case_id=case_id,
    )


# ============================================================================
# WEB SCRAPER ENDPOINTS (services.ecourts.gov.in)
# ============================================================================

class WebCaseDetailsRequest(BaseModel):
    case_no: str
    cino: str
    court_code: str
    hideparty: str
    search_flag: str
    state_code: str
    dist_code: str
    court_complex_code: str
    search_by: str

@router.get("/dc/states/", summary="List all states from eCourts web")
async def get_web_states():
    scraper = EcourtsWebScraper()
    if scraper.initialize_session():
        return scraper.get_states()
    return {"error": "Failed to initialize session"}


@router.get("/dc/districts/{state_code}", summary="List districts from eCourts web")
async def get_web_districts(state_code: str):
    scraper = EcourtsWebScraper()
    if scraper.initialize_session():
        return scraper.get_districts(state_code)
    return {"error": "Failed to initialize session"}


@router.get("/dc/court_complexes/{state_code}/{dist_code}", summary="List court complexes from eCourts web")
async def get_web_court_complexes(state_code: str, dist_code: str):
    scraper = EcourtsWebScraper()
    if scraper.initialize_session():
        return scraper.get_court_complexes(state_code, dist_code)
    return {"error": "Failed to initialize session"}


@router.get("/dc/case_types/{state_code}/{dist_code}/{complex_code}", summary="List case types from eCourts web")
async def get_web_case_types(state_code: str, dist_code: str, complex_code: str, est_code: str = ""):
    scraper = EcourtsWebScraper()
    if scraper.initialize_session():
        return scraper.get_case_types(state_code, dist_code, complex_code, est_code)
    return {"error": "Failed to initialize session"}


@router.get("/dc/search_by_case_number/", summary="Search case from eCourts web")
async def web_search_by_case_number(
    state_code: str,
    dist_code: str,
    complex_code: str,
    case_type: str,
    case_no: str,
    year: str,
):
    scraper = EcourtsWebScraper()
    if scraper.initialize_session():
        print(f"Searching eCourts web with state_code={state_code}, dist_code={dist_code}, complex_code={complex_code}, case_type={case_type}, case_no={case_no}, year={year}")
        return scraper.search_case(
            state_code, dist_code, complex_code, case_type, case_no, year
        )
    return {"error": "Failed to initialize session"}


@router.post("/dc/case_details/", summary="Get case details from eCourts web")
async def web_case_details(params: WebCaseDetailsRequest):
    scraper = EcourtsWebScraper()
    if scraper.initialize_session():
        return scraper.get_case_details(params.dict())
    return {"error": "Failed to initialize session"}


@router.post("/store_orders/", summary="Store fetched orders once a case is saved")
async def store_orders(
    orders: list[dict] | None = Body(default=None),
    case_id: str | None = Body(default=None),
    court_type: str | None = Body(default=None),
):
    court_key = (court_type or "").strip().upper()
    if court_key == "NCLT":
        stored_orders = await nclt_persist_orders_to_storage(
            orders,
            case_id=case_id,
        )
    elif court_key == "NCLAT":
        stored_orders = await nclat_persist_orders_to_storage(
            orders,
            case_id=case_id,
        )
    elif court_key in {"BOMBAY_HC", "BHC", "MH"}:
        stored_orders = await bombay_persist_orders_to_storage(
            orders,
            case_id=case_id,
        )
    elif court_key in {"GUJARAT_HC", "GJHC", "GJ"}:
        stored_orders = await gujarat_persist_orders_to_storage(
            orders,
            case_id=case_id,
        )
    elif court_key in {"DELHI_HC", "DLHC", "DH", "DL"}:
        stored_orders = await delhi_persist_orders_to_storage(
            orders,
            case_id=case_id,
        )
    elif court_key == "WEB_ECOURTS":
        stored_orders = await web_persist_orders_to_storage(
            orders,
            case_id=case_id,
        )
    else:
        stored_orders = await hc_services.persist_orders_to_storage(
            orders,
            case_id=case_id,
        )
    return stored_orders or []


if __name__ == "__main__":
    import uvicorn

    uvicorn.run("ecourts.router:router", host="0.0.0.0", port=8000)

# https://github.com/Tejaswgupta/votum_fastapi_oai.git
