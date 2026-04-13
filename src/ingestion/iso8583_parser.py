"""
===========================================================================
Module : iso8583_parser_full.py
Projet : Plateforme MLOps - Détection de Fraude (Al Barid Bank)
Auteur : Hamza (PFE EMSI 2025-2026)
---------------------------------------------------------------------------
Rôle   : Parser COMPLET ISO 8583 → JSON structuré (128 Data Elements).
         Les champs absents du message sont remplis avec null.
         Utilisé en sortie du topic Kafka "raw_stream" (Ingestion Layer).
Stack  : pyiso8583 v4.0.1+ (spec ISO 8583:1987)
===========================================================================
"""

import json
import logging
from typing import Union

import iso8583
from iso8583.specs import default as spec87

# ---------------------------------------------------------------------------
# Logging
# ---------------------------------------------------------------------------
logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s | %(levelname)-7s | %(name)s | %(message)s",
)
logger = logging.getLogger("iso8583_parser_full")

# ---------------------------------------------------------------------------
# Mapping COMPLET des 128 Data Elements ISO 8583:1987
# ---------------------------------------------------------------------------
FIELD_MAP: dict[str, str] = {
    "t":   "mti",
    "p":   "bitmap_primary",
    "1":   "bitmap_secondary",
    "2":   "pan",
    "3":   "processing_code",
    "4":   "amount_transaction",
    "5":   "amount_settlement",
    "6":   "amount_cardholder_billing",
    "7":   "transmission_datetime",
    "8":   "amount_cardholder_billing_fee",
    "9":   "conversion_rate_settlement",
    "10":  "conversion_rate_cardholder_billing",
    "11":  "stan",
    "12":  "time_local",
    "13":  "date_local",
    "14":  "date_expiration",
    "15":  "date_settlement",
    "16":  "date_conversion",
    "17":  "date_capture",
    "18":  "mcc",
    "19":  "acquiring_country_code",
    "20":  "pan_country_code",
    "21":  "forwarding_country_code",
    "22":  "pos_entry_mode",
    "23":  "pan_sequence_number",
    "24":  "nii",
    "25":  "pos_condition_code",
    "26":  "pos_capture_code",
    "27":  "auth_id_response_length",
    "28":  "amount_transaction_fee",
    "29":  "amount_settlement_fee",
    "30":  "amount_transaction_processing_fee",
    "31":  "amount_settlement_processing_fee",
    "32":  "acquiring_institution_id",
    "33":  "forwarding_institution_id",
    "34":  "pan_extended",
    "35":  "track2_data",
    "36":  "track3_data",
    "37":  "retrieval_reference_number",
    "38":  "authorization_id_response",
    "39":  "response_code",
    "40":  "service_restriction_code",
    "41":  "terminal_id",
    "42":  "merchant_id",
    "43":  "card_acceptor_name_location",
    "44":  "additional_response_data",
    "45":  "track1_data",
    "46":  "additional_data_iso",
    "47":  "additional_data_national",
    "48":  "additional_data_private",
    "49":  "currency_code_transaction",
    "50":  "currency_code_settlement",
    "51":  "currency_code_cardholder_billing",
    "52":  "pin_data",
    "53":  "security_control_info",
    "54":  "additional_amounts",
    "55":  "icc_data",
    "56":  "reserved_iso_56",
    "57":  "reserved_national_57",
    "58":  "reserved_national_58",
    "59":  "reserved_national_59",
    "60":  "reserved_national_60",
    "61":  "reserved_private_61",
    "62":  "reserved_private_62",
    "63":  "reserved_private_63",
    "64":  "mac",
    "65":  "bitmap_extended",
    "66":  "settlement_code",
    "67":  "extended_payment_code",
    "68":  "receiving_country_code",
    "69":  "settlement_country_code",
    "70":  "network_management_info_code",
    "71":  "message_number",
    "72":  "message_number_last",
    "73":  "date_action",
    "74":  "credits_number",
    "75":  "credits_reversal_number",
    "76":  "debits_number",
    "77":  "debits_reversal_number",
    "78":  "transfer_number",
    "79":  "transfer_reversal_number",
    "80":  "inquiries_number",
    "81":  "authorizations_number",
    "82":  "credits_processing_fee_amount",
    "83":  "credits_transaction_fee_amount",
    "84":  "debits_processing_fee_amount",
    "85":  "debits_transaction_fee_amount",
    "86":  "credits_amount",
    "87":  "credits_reversal_amount",
    "88":  "debits_amount",
    "89":  "debits_reversal_amount",
    "90":  "original_data_elements",
    "91":  "file_update_code",
    "92":  "file_security_code",
    "93":  "response_indicator",
    "94":  "service_indicator",
    "95":  "replacement_amounts",
    "96":  "message_security_code",
    "97":  "amount_net_settlement",
    "98":  "payee",
    "99":  "settlement_institution_id",
    "100": "receiving_institution_id",
    "101": "file_name",
    "102": "account_id_1",
    "103": "account_id_2",
    "104": "transaction_description",
    "105": "reserved_iso_105",
    "106": "reserved_iso_106",
    "107": "reserved_iso_107",
    "108": "reserved_iso_108",
    "109": "reserved_iso_109",
    "110": "reserved_iso_110",
    "111": "reserved_iso_111",
    "112": "reserved_national_112",
    "113": "reserved_national_113",
    "114": "reserved_national_114",
    "115": "reserved_national_115",
    "116": "reserved_national_116",
    "117": "reserved_national_117",
    "118": "reserved_national_118",
    "119": "reserved_national_119",
    "120": "reserved_private_120",
    "121": "reserved_private_121",
    "122": "reserved_private_122",
    "123": "reserved_private_123",
    "124": "reserved_private_124",
    "125": "reserved_private_125",
    "126": "reserved_private_126",
    "127": "reserved_private_127",
    "128": "mac_2",
}

# Liste ordonnée : MTI, bitmap, puis DE 1 → 128
ORDERED_KEYS = ["t", "p"] + [str(i) for i in range(1, 129)]


def parse_iso_to_json(
    raw_data: Union[bytes, bytearray],
    pretty: bool = True,
) -> str:
    """
    Décode une trame ISO 8583 et retourne un JSON COMPLET (128 DEs).
    Les champs absents du message sont remplis avec null.

    Parameters
    ----------
    raw_data : bytes | bytearray
        Trame ISO 8583 brute.
    pretty : bool
        Indentation JSON si True.

    Returns
    -------
    str
        JSON structuré avec les 128 DEs + MTI + bitmap.
    """
    try:
        doc_dec, _doc_enc = iso8583.decode(raw_data, spec87)
        logger.info(
            "Trame décodée — MTI=%s | STAN=%s | Champs présents=%d",
            doc_dec.get("t", "?"),
            doc_dec.get("11", "N/A"),
            len(doc_dec),
        )
    except iso8583.DecodeError as exc:
        logger.error("Erreur de décodage ISO 8583 : %s", exc)
        raise

    # Construction du JSON complet : chaque DE absent → null
    output = {}
    for key in ORDERED_KEYS:
        label = FIELD_MAP.get(key, f"de_{key}")
        output[label] = doc_dec.get(key, None)

    indent = 2 if pretty else None
    return json.dumps(output, indent=indent, ensure_ascii=False, default=str)


def parse_iso_to_dict(
    raw_data: Union[bytes, bytearray],
) -> dict:
    """
    Même logique que parse_iso_to_json mais retourne un dict Python.
    Utile pour alimenter directement Spark / Pandas / FastAPI.
    """
    doc_dec, _ = iso8583.decode(raw_data, spec87)

    output = {}
    for key in ORDERED_KEYS:
        label = FIELD_MAP.get(key, f"de_{key}")
        value = doc_dec.get(key, None)
        # Convertir bytes en str pour compatibilité
        if isinstance(value, (bytes, bytearray)):
            value = value.hex().upper()
        output[label] = value

    return output





# ---------------------------------------------------------------------------
# Point d'entrée
# ---------------------------------------------------------------------------

if __name__ == "__main__":
    rawr = "30323030f23c46d12ee09400040000000400000031363431343733333132333435363738393030313030303030303030303030323030303030343133313533303030303030303432313533303030303431333239303336303131303531303031303031324330303030303030303036303037333530333334313437333331323334353637383930443239303331303130303030303030303030343133313533303030343237383930313230304741423230303837424152494442414e4b3030303038374741422042415249442042414e4b204f554a444120202020204f554a444120202020202020204d41353034aabb0011223344353031373035303443303030303030303230303030333031313330303130303132333435363738"


    print("=" * 70)
    print("  PARSER ISO 8583 COMPLET — 128 Data Elements")
    print("=" * 70)
    print(f"  Trame HEX : {rawr}")
    print(f"  Taille    : {len(rawr)} octets")
    rawt = bytes.fromhex(rawr)
    print("=" * 70)
    print()
    print(parse_iso_to_json(rawt))

    # # Statistiques
    # d = parse_iso_to_dict(raw)
    # present = sum(1 for v in d.values() if v is not None)
    # absent  = sum(1 for v in d.values() if v is None)
    # print()
    # print(f"  Résumé : {present} champs présents | {absent} champs null")
























































# """
# ================================================================================
#  ISO 8583 Parser — Version Corrigée (Sans erreurs)
#  Simple et directe
# ================================================================================
# """
#
# import json
#
#
# def parse_iso8583_simple(raw_data):
#     """
#     Parse une trame ISO 8583 simplement.
#
#     Args:
#         raw_data: bytes ou str - La trame brute
#
#     Returns:
#         dict - Les données parsées
#     """
#
#     # Convertir bytes en string
#     if isinstance(raw_data, bytes):
#         data = raw_data.decode('utf-8')
#     else:
#         data = raw_data
#
#     print(f"📥 TRAME REÇUE: {data}\n")
#
#     # ÉTAPE 1 : Extraire le MTI (4 caractères)
#     mti = data[0:4]
#     print(f"1️⃣ MTI : {mti}")
#
#     # ÉTAPE 2 : Extraire le Bitmap (16 caractères)
#     bitmap_hex = data[4:20]
#     print(f"2️⃣ Bitmap (HEX) : {bitmap_hex}\n")
#
#     # ÉTAPE 3 : Extraire les Data Elements
#     print("3️⃣ Data Elements extraits :\n")
#
#     # Pour cet exemple, positions fixes
#     de_2 = data[20:33] if len(data) > 20 else ""
#     de_3 = data[33:39] if len(data) > 33 else ""
#     de_4 = data[39:51] if len(data) > 39 else ""
#     de_7 = data[51:61] if len(data) > 51 else ""
#     de_12 = data[61:67] if len(data) > 61 else ""
#     de_39 = data[67:69] if len(data) > 67 else ""
#
#     print(f"   DE_2 (PAN): {de_2}")
#     print(f"   DE_3 (Processing Code): {de_3}")
#     print(f"   DE_4 (Amount): {de_4}")
#     print(f"   DE_7 (DateTime): {de_7}")
#     print(f"   DE_12 (Time): {de_12}")
#     print(f"   DE_39 (Response): {de_39}\n")
#
#     # ÉTAPE 4 : Construire le résultat JSON
#     result = {
#         "MTI": mti,
#         "MTI_description": "Authorization Request" if mti == "0100" else "Unknown",
#         "primary_bitmap_hex": bitmap_hex,
#         "data_elements": {
#             "DE_2": {
#                 "value": de_2,
#                 "name": "Primary Account Number (PAN)",
#                 "position": 20,
#                 "length": 13
#             },
#             "DE_3": {
#                 "value": de_3,
#                 "name": "Processing Code",
#                 "position": 33,
#                 "length": 6
#             },
#             "DE_4": {
#                 "value": de_4,
#                 "name": "Amount",
#                 "position": 39,
#                 "length": 12
#             },
#             "DE_7": {
#                 "value": de_7,
#                 "name": "Transmission Date/Time",
#                 "position": 51,
#                 "length": 10
#             },
#             "DE_12": {
#                 "value": de_12,
#                 "name": "Local Transaction Time",
#                 "position": 61,
#                 "length": 6
#             },
#             "DE_39": {
#                 "value": de_39,
#                 "name": "Response Code",
#                 "position": 67,
#                 "length": 2
#             }
#         },
#         "active_de_count": 6
#     }
#
#     return result
#
#
# def main():
#     """Test du parser."""
#
#     print("=" * 80)
#     print("  ISO 8583 PARSER — VERSION SIMPLE")
#     print("=" * 80)
#     print()
#
#     # Trame de test
#     raw_trame = b'0100423000000080000081043118101422301234567890123456'
#
#     # Parser
#     result = parse_iso8583_simple(raw_trame)
#
#     # Afficher le JSON
#     print("=" * 80)
#     print("  ✅ RÉSULTAT JSON")
#     print("=" * 80)
#     print()
#     print(json.dumps(result, indent=2))
#     print()
#
#     # Analyse
#     print("=" * 80)
#     print("  📊 ANALYSE")
#     print("=" * 80)
#     print()
#     print(f"Type de transaction : {result['MTI_description']}")
#     print(f"Data Elements actifs : {result['active_de_count']}")
#
#     # Montant
#     if result['data_elements']['DE_4']['value']:
#         amount_str = result['data_elements']['DE_4']['value']
#         amount = float(amount_str) / 100.0
#         print(f"Montant : {amount:.2f} MAD")
#
#     # PAN
#     if result['data_elements']['DE_2']['value']:
#         pan = result['data_elements']['DE_2']['value']
#         pan_masked = pan[:4] + "*" * (len(pan) - 8) + pan[-4:]
#         print(f"Carte : {pan_masked}")
#
#     print()
#
#
# if __name__ == "__main__":
#     main()