from fastapi import APIRouter 
from src.schemas import SubscriptionCreate 

router=APIRouter()


@router.post("/")
def subscribe(payload: SubscriptionCreate):
    return {"message:" "Subscription Createed"}