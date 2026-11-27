from feast import Entity, ValueType

customer = Entity(
    name="customer",
    join_keys=["client_id"],
    value_type=ValueType.INT64,
    description="Khách hàng (users_data.id, transactions_data.client_id)",
    tags={"owner": "risk-data"},
)
