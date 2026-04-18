import uuid
import logging

from common import message_protocol


# Cambio el message handler para poder iderntificar el cliente
# con un client id - corellation id
# ya no mando simplemente [fruta, cantidad], sino que le agrego el client id 
# tmb dejo de mandar EOF vacios, sino que agrego el client id
# cuando me llega el resultado, lo deserializo y verifico que el client id sea el mismo

RESULT_PARAMS_AMOUNT = 2

class MessageHandler:

    def __init__(self):
        self.client_id = str(uuid.uuid4())

    def serialize_data_message(self, message):
        [fruit, amount] = message
        logging.info(f"Gateway: Serializing data for client {self.client_id}: fruit={fruit}, amount={amount}")
        return message_protocol.internal.serialize_client_data(
            self.client_id, fruit, amount
        )

    def serialize_eof_message(self, message):
        logging.info(f"Gateway: Serializing EOF for client {self.client_id}")
        return message_protocol.internal.serialize_client_eof(self.client_id)

    def deserialize_result_message(self, message):
        fields = message_protocol.internal.deserialize(message)
        if (
            isinstance(fields, list)
            and len(fields) == RESULT_PARAMS_AMOUNT
            and fields[0] == self.client_id
        ):
            logging.info(f"Gateway: Deserialized result for client {self.client_id}: top={fields[1]}")
            return fields[1]
        logging.warning(f"Gateway: Invalid result message for client {self.client_id}: {fields}")
        return []
