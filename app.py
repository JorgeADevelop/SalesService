from chalice import Chalice, Response
from sqlalchemy import create_engine, func, Column, Integer, String, Float, DateTime, ForeignKey
from sqlalchemy.orm import Session, relationship
from sqlalchemy.ext.declarative import declarative_base
from marshmallow import Schema, fields
import os

app = Chalice(app_name='SalesService')

if os.environ.get("DEBUG", False):
    app.debug = True

db_user = os.environ.get("DB_USER")
db_password = os.environ.get("DB_PASSWORD")
db_host = os.environ.get("DB_HOST")
db_port = os.environ.get("DB_PORT")
db_name = os.environ.get("DB_NAME")

engine = create_engine(f"postgresql+psycopg2://{db_user}:{db_password}@{db_host}:{db_port}/{db_name}", echo=True)

Base = declarative_base()

messages = {
    "SaleCreated": "The sale has been created successfully",
    "RecordFound": "The {resource} has been found successfully",
    "RecordNotFound": "The {resource} with id '{id}' has not been found",
}


class Product(Base):
    __tablename__ = "products"

    id = Column(Integer, primary_key=True, nullable=False)
    name = Column(String, nullable=False, unique=True)
    price = Column(Float, nullable=False)

    sales = relationship('Sale', backref='products', lazy='dynamic')


class Sale(Base):
    __tablename__ = "sales"

    id = Column(Integer, primary_key=True, nullable=False)
    quantity = Column(Integer, nullable=False)
    amount = Column(Float, nullable=False)
    product_id = Column(Integer, ForeignKey('products.id', ondelete='SET NULL'), nullable=True)
    created_at = Column(DateTime, nullable=False, server_default=func.now())

    product = relationship(Product, backref="product")


class ProductSchema(Schema):
    id = fields.Integer()
    name = fields.String()
    price = fields.Float()
    unit_measure_id = fields.Integer()


class SaleSchema(Schema):
    id = fields.Integer()
    quantity = fields.Integer()
    amount = fields.Float()
    product_id = fields.Integer()
    created_at = fields.DateTime()
    product = fields.Nested(ProductSchema)


Base.metadata.create_all(engine)


@app.route('/sales', methods=['GET'])
def index():
    try:
        offset = app.current_request.query_params.get("offset", 0)
        limit = app.current_request.query_params.get("limit", 10)
        sales = []
        totalRecords = 0

        with Session(engine) as session:
            for data in session.query(Sale).offset(offset).limit(limit).all():
                sales.append(SaleSchema().dump(data))
                totalRecords = session.query(Sale).count()
        return MakeResponsePaginate(
            message=messages.get("RecordFound").format(resource="sales"),
            data=sales,
            totalRecords=totalRecords
        )
    except KeyError as e:
        return e
    return "aja"


@app.route('/sale', methods=['POST'])
def makeSale():
    try:
        json_body = app.current_request.json_body
        sale = Sale(
            quantity=json_body.get("quantity"),
            product_id=json_body.get("product_id")
        )

        with Session(engine) as session:
            product = session.query(Product).where(Product.id == sale.product_id).first()
            if product is None:
                return MakeResponse(
                    message=messages.get("RecordNotFound").format(resource="product", id=id),
                    status_code=400
                )
            sale.amount = sale.quantity * product.price
            session.add(sale)
            session.flush()
            sale = SaleSchema().dump(sale)
            session.commit()

        return MakeResponse(
            message=messages.get("SaleCreated"),
            data=sale
        )
    except KeyError as e:
        return e


@app.route('/sales-by-product/{product_id}', methods=['GET'])
def indexByProduct(product_id):
    try:
        offset = app.current_request.query_params.get("offset", 0)
        limit = app.current_request.query_params.get("limit", 10)
        sales = []
        totalRecords = 0

        with Session(engine) as session:
            for data in session.query(Sale).where(Sale.product_id == product_id).offset(offset).limit(limit).all():
                sales.append(SaleSchema().dump(data))
                totalRecords = session.query(Sale).where(Sale.product_id == product_id).count()
        return MakeResponsePaginate(
            message=messages.get("RecordFound").format(resource="sales"),
            data=sales,
            totalRecords=totalRecords
        )
    except KeyError as e:
        return e
    return "aja"


def MakeResponse(message, data=None, status_code=200, error=None):
    status = "OK"
    if status_code == 400:
        status = "BadRequest"
    elif status_code == 500:
        status = "InternalServerError"

    return Response(body={
        "status": status,
        "code": status_code,
        "message": message,
        "error": error,
        "data": data,
    })


def MakeResponsePaginate(message, data, totalRecords):
    return Response(body={
        "status": "OK",
        "code": 200,
        "message": message,
        "error": None,
        "data": data,
        "total_records": totalRecords,
    })
