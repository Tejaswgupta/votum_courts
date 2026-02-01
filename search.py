from typing import Any, List, Optional, Union

from pydantic import BaseModel


class SearchResultData(BaseModel):
    cino: str
    date_of_decision: str
    pet_name: str
    res_name: str
    type_name: str
    

class Order(BaseModel):
    hearing_date: Optional[str]
    purpose: Optional[str]
    order_link: Optional[str]


class HistoryData(BaseModel):
    judge: Optional[str] = None
    business_date: Optional[str] = None
    hearing_date: Optional[str]
    purpose: Optional[str]


class Act(BaseModel):
    Under_Act_s: Optional[str]
    Under_Section_s: Optional[str]


class ConciseJson(BaseModel):
    cin_no: Optional[str] = None
    registration_no: Optional[str] = None
    filling_no: Optional[str] = None
    case_no: Optional[str]
    registration_date: Optional[str] = None
    filing_date: Optional[str]
    first_listing_date: Optional[str] = None
    next_listing_date: Optional[str] = None
    last_listing_date: Optional[str] = None
    decision_date: Optional[str] = None
    court_no: Optional[str] = None

    disposal_nature: Optional[int] = None
    purpose_next: Optional[str] = None
    case_type: Optional[str] = None
    petitioner: Optional[str] 
    respondent: Optional[str]
    petitioner_advocates: Optional[str]
    respondent_advocates: Optional[str]

    judges: Optional[str] = None
    bench_name: Optional[str] = None
    court_name: Optional[str] = None
    history: Optional[List[HistoryData]] = None
    acts: Optional[Act] = None
    orders: Optional[Any] = None

    additional_info: Optional[Any] = None
    original_json: Optional[Any] = None
