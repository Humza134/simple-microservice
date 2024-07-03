from fastapi import HTTPException, Request
from sqlmodel import Session, select
from app.models.product_model import Product, ProductUpdate
import requests


# call the category service to fetch the category with associated products

def get_category_by_id(category_id: int, request: Request):
    category_service_url = f"http://category_service:8006/manage-category/{category_id}"
    response = requests.get(category_service_url)
    if response.status_code == 404:
        return None
    return response.json()

# Add a new product in database

def add_new_product(product_data: Product, session: Session):
    print("Adding products in database")
    session.add(product_data)
    session.commit()
    session.refresh(product_data)
    return product_data

# Get all products

def get_all_products(request: Request, session: Session):
    all_products = session.exec(select(Product)).all()
    for product in all_products:
        product.category = get_category_by_id(product.category_id, request)
    return all_products

# Get product by id

def get_product_by_id(product_id: int, request: Request, session: Session):
    product = session.exec(select(Product).where(Product.id == product_id)).one_or_none()
    if product is None:
        raise HTTPException(status_code=404, detail="Product is not found")
    product.category = get_category_by_id(product.category_id, request)
    return product

# Delete product by id

def delete_product_by_id(product_id: int, session: Session):
    product = session.exec(select(Product).where(Product.id == product_id)).one_or_none()
    if product is None:
        raise HTTPException(status_code=404, detail="Product is not found")
    session.delete(product)
    session.commit()
    return {"message": "Product deleted successfully"}

# update product

def update_product_by_id(product_id: int, to_update_product_data: ProductUpdate, session: Session):
    product = session.exec(select(Product).where(Product.id == product_id)).one_or_none()
    if product is None:
        raise HTTPException(status_code=404, detail="Product is not found")
    # update the product
    update_product = to_update_product_data.model_dump(exclude_unset=True)
    product.sqlmodel_update(update_product)
    session.add(product)
    session.commit()
    return product
