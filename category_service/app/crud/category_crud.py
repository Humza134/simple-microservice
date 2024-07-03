from fastapi import HTTPException, Request
from sqlmodel import Session, select
from app.models.category_model import Category, CategoryUpdate
import requests


# call the product service to fetch the products with associated category

def get_product_by_category(category_id: int, request: Request):
    product_service_url = f"http://product_service:8005/products/by-category/{category_id}"
    response = requests.get(product_service_url)
    if response.status_code == 404:
        return []
    return response.json()

# Add a new product in database

def add_category(category_data: Category, session: Session):
    print("Adding category in database")
    session.add(category_data)
    session.commit()
    session.refresh(category_data)
    return category_data

# Get all products

def get_all_categories(request: Request, session: Session):
    all_categories = session.exec(select(Category)).all()
    for category in all_categories:
        category.products = get_product_by_category(category.id, request)
    return all_categories

# Get product by id

def get_category_by_id(category_id: int, request: Request, session: Session):
    category = session.exec(select(Category).where(Category.id == category_id)).one_or_none()
    if category is None:
        raise HTTPException(status_code=404, detail="category is not found")
    category.products = get_product_by_category(category_id, request)
    return category

# Delete product by id

def delete_category_by_id(category_id: int, session: Session):
    category = session.exec(select(Category).where(Category.id == category_id)).one_or_none()
    if category is None:
        raise HTTPException(status_code=404, detail="category is not found")
    session.delete(category)
    session.commit()
    return {"message": "category deleted successfully"}

# update product

def update_category_by_id(category_id: int, to_update_category_data: CategoryUpdate, session: Session):
    category = session.exec(select(Category).where(Category.id == category_id)).one_or_none()
    if category is None:
        raise HTTPException(status_code=404, detail="category is not found")
    # update the category
    update_category = to_update_category_data.model_dump(exclude_unset=True)
    category.sqlmodel_update(update_category)
    session.add(category)
    session.commit()
    return category
