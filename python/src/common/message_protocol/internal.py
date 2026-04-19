import json


def serialize(message):
    return json.dumps(message).encode("utf-8")


def deserialize(message):
    return json.loads(message.decode("utf-8"))


# defino contrato cliente <-> pipeline interno 
# datos: [client_id, fruta, cantidad]
# EOF desde gateway: [client_id] (un solo elemento; client_id)
# resultado hacia gateway (join -> cola de salida): [client_id, top]
# donde top es [[fruta, cantidad], ...]


def serialize_client_data(client_id, fruit, amount):
    return serialize([client_id, fruit, amount])


def serialize_client_eof(client_id, sum_id):
    return serialize([client_id, sum_id])


def serialize_client_result(client_id, fruit_top_rows):
    return serialize([client_id, fruit_top_rows])

def serialize_client_eof_signal(client_id, sum_id):
    # mando el EOF signal con el client id y el id del sum que lo envía, para que los otros Sums sepan que tienen que hacer flush de ese cliente
    return serialize([client_id, "EOF_SIGNAL", sum_id])

def deserialize_control_signal(message):
    data = deserialize(message)
    return data[0] 