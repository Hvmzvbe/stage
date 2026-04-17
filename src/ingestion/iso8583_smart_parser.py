import copy
import json
import logging
from typing import Union

import iso8583
from iso8583.specs import default as _base_spec87_binary
from iso8583.specs import default_ascii as _base_spec87_ascii

# ---------------------------------------------------------------------------
# Logging
# ---------------------------------------------------------------------------
logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s | %(levelname)-7s | %(name)s | %(message)s",
)
logger = logging.getLogger("iso8583_parser")


# ===========================================================================
#  CONSTRUCTION DES 6 SPECS
# ===========================================================================

# ---- 1. spec87 (ISO 8583:1987) — Fourni nativement par pyiso8583 ----
spec87_binary = _base_spec87_binary
spec87_ascii  = _base_spec87_ascii


# ---- 2. spec93 (ISO 8583:1993) — Construit depuis spec87 ----
spec93_binary = copy.deepcopy(spec87_binary)
# DE 26 : POS Capture Code élargi à 4 (était 2 en spec87)
spec93_binary["26"]["max_len"] = 4
spec93_binary["26"]["desc"] = "Point-of-Service Capture Code (1993)"
# DE 33 : Forwarding Institution ID élargi à 28
spec93_binary["33"]["max_len"] = 28
spec93_binary["33"]["desc"] = "Forwarding Institution ID Code (1993)"
# DE 55 : ICC/EMV Data — LLLVAR binaire jusqu'à 999
spec93_binary["55"] = {
    "data_enc": "b",
    "len_enc": "ascii",
    "len_type": 3,
    "max_len": 999,
    "desc": "ICC System-Related Data (EMV)",
}
# DE 100 : Receiving Institution ID élargi à 24
spec93_binary["100"]["max_len"] = 24
spec93_binary["100"]["desc"] = "Receiving Institution ID Code (1993)"

# Variante ASCII de spec93
spec93_ascii = copy.deepcopy(spec93_binary)
for field in ["p", "1", "52", "55", "64", "96", "128"]:
    if field in spec93_ascii:
        spec93_ascii[field]["data_enc"] = "ascii"
        if spec93_ascii[field]["max_len"] <= 16:
            spec93_ascii[field]["max_len"] = 16


# ---- 3. spec03 (ISO 8583:2003) — Construit depuis spec93 ----
spec03_binary = copy.deepcopy(spec93_binary)
# DE 33 : Forwarding Institution ID élargi à 28
spec03_binary["33"]["max_len"] = 28
spec03_binary["33"]["desc"] = "Forwarding Institution ID Code (2003)"
# DE 34 : PAN Extended élargi à 28
spec03_binary["34"]["max_len"] = 28
spec03_binary["34"]["desc"] = "PAN, Extended (2003)"
# DE 48 : Additional Data Private (LLLVAR 999)
spec03_binary["48"]["max_len"] = 999
spec03_binary["48"]["desc"] = "Additional Data - Private (2003)"
# DE 56 : Original Data Elements redéfini en LLLVAR
spec03_binary["56"] = {
    "data_enc": "ascii",
    "len_enc": "ascii",
    "len_type": 3,
    "max_len": 999,
    "desc": "Original Data Elements (2003)",
}

# Variante ASCII de spec03
spec03_ascii = copy.deepcopy(spec03_binary)
for field in ["p", "1", "52", "55", "64", "96", "128"]:
    if field in spec03_ascii:
        spec03_ascii[field]["data_enc"] = "ascii"
        if spec03_ascii[field]["max_len"] <= 16:
            spec03_ascii[field]["max_len"] = 16


# ===========================================================================
#  REGISTRE DES SPECS
# ===========================================================================

SPEC_REGISTRY = {
    "spec87_binary": spec87_binary,
    "spec87_ascii":  spec87_ascii,
    "spec93_binary": spec93_binary,
    "spec93_ascii":  spec93_ascii,
    "spec03_binary": spec03_binary,
    "spec03_ascii":  spec03_ascii,
}

# Mapping MTI version digit → spec version
MTI_VERSION_MAP = {
    "0": "spec87",   # ISO 8583:1987
    "1": "spec93",   # ISO 8583:1993
    "2": "spec03",   # ISO 8583:2003
}

MTI_VERSION_LABELS = {
    "0": "ISO 8583:1987",
    "1": "ISO 8583:1993",
    "2": "ISO 8583:2003",
}

# Caractères hex valides
HEX_CHARS = set(b"0123456789ABCDEFabcdef")


# ===========================================================================
#  MAPPING COMPLET DES 128 DATA ELEMENTS
# ===========================================================================

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

ORDERED_KEYS = ["t", "p"] + [str(i) for i in range(1, 129)]


# ===========================================================================
#  SMART SPEC DETECTION (spec87 / spec93 / spec03 x binary / ascii)
# ===========================================================================

def detect_spec(raw_data: Union[bytes, bytearray]) -> str:
    """
    Détecte automatiquement le spec ISO 8583 d'une trame brute.

    Stratégie de détection en 2 étapes :
    ─────────────────────────────────────
    ÉTAPE 1 — Version ISO (depuis le MTI) :
        Le premier digit du MTI (byte 0) identifie la version :
          '0' → ISO 8583:1987  (spec87)
          '1' → ISO 8583:1993  (spec93)
          '2' → ISO 8583:2003  (spec03)

    ÉTAPE 2 — Encodage du bitmap (binary vs ASCII) :
        Après le MTI (4 bytes), le bitmap commence.
          Binary : 8 octets bruts (peuvent contenir des non-printable)
          ASCII  : 16 caractères hex (toujours 0-9, A-F)
        On lit les 16 octets bytes[4:20] :
          → Si TOUS sont des hex ASCII → bitmap ASCII
          → Sinon → bitmap binaire

    Parameters
    ----------
    raw_data : bytes | bytearray

    Returns
    -------
    str
        Ex: "spec87_binary", "spec93_ascii", "spec03_binary"

    Raises
    ------
    ValueError
        Trame trop courte, MTI invalide, ou version inconnue.
    """
    # ── Validation minimale ──
    if len(raw_data) < 12:
        raise ValueError(
            f"Trame trop courte ({len(raw_data)} octets). "
            f"Minimum : 12 octets (MTI + bitmap binaire)."
        )

    # ── ÉTAPE 1 : Version ISO depuis le MTI ──
    mti_bytes = raw_data[0:4]
    try:
        mti = mti_bytes.decode("ascii")
    except UnicodeDecodeError:
        raise ValueError(f"MTI non-ASCII : {mti_bytes.hex()}")

    if not mti.isdigit():
        raise ValueError(f"MTI invalide (non numérique) : '{mti}'")

    version_digit = mti[0]
    spec_version = MTI_VERSION_MAP.get(version_digit)

    if spec_version is None:
        raise ValueError(
            f"Version ISO inconnue dans le MTI '{mti}'. "
            f"Premier digit '{version_digit}' non reconnu. "
            f"Attendu : '0' (1987), '1' (1993), '2' (2003)."
        )

    iso_label = MTI_VERSION_LABELS[version_digit]

    # ── ÉTAPE 2 : Encodage du bitmap ──
    bitmap_zone = raw_data[4:20]

    if len(bitmap_zone) >= 16:
        is_ascii = all(byte in HEX_CHARS for byte in bitmap_zone)
    else:
        is_ascii = False

    if is_ascii:
        try:
            bitmap_hex = bitmap_zone.decode("ascii")
            bitmap_int = int(bitmap_hex, 16)
            if bitmap_int == 0:
                is_ascii = False
        except (UnicodeDecodeError, ValueError):
            is_ascii = False

    bitmap_type = "ascii" if is_ascii else "binary"
    spec_name = f"{spec_version}_{bitmap_type}"

    logger.info(
        "DETECTION — MTI=%s | Version=%s | Bitmap=%s | Spec=%s",
        mti, iso_label, bitmap_type, spec_name,
    )

    return spec_name


def get_spec_info(spec_name: str) -> dict:
    """Retourne les métadonnées descriptives d'un spec détecté."""
    parts = spec_name.split("_")
    version_key = parts[0]
    bitmap_enc = parts[1]

    version_map = {
        "spec87": {"iso_version": "1987", "label": "ISO 8583:1987"},
        "spec93": {"iso_version": "1993", "label": "ISO 8583:1993"},
        "spec03": {"iso_version": "2003", "label": "ISO 8583:2003"},
    }

    info = version_map.get(version_key, {})
    info["bitmap_encoding"] = bitmap_enc
    info["spec_name"] = spec_name
    return info


# ===========================================================================
#  PARSER PRINCIPAL
# ===========================================================================

def parse_iso_to_json(
    raw_data: Union[bytes, bytearray],
    pretty: bool = True,
) -> str:
    """
    Parse intelligent : détecte le spec (87/93/03 x binary/ascii),
    décode la trame et retourne un JSON complet (128 DEs, absents = null).
    """
    spec_name = detect_spec(raw_data)
    spec = SPEC_REGISTRY[spec_name]
    spec_info = get_spec_info(spec_name)

    try:
        doc_dec, _ = iso8583.decode(raw_data, spec)
        fields_present = len(doc_dec)
        logger.info(
            "Décodé avec %s — MTI=%s | STAN=%s | Champs=%d",
            spec_name, doc_dec.get("t", "?"),
            doc_dec.get("11", "N/A"), fields_present,
        )
    except iso8583.DecodeError as exc:
        logger.error("Échec décodage avec %s : %s", spec_name, exc)
        raise

    output = {
        "_metadata": {
            "spec_detected": spec_name,
            "iso_version": spec_info["iso_version"],
            "iso_label": spec_info["label"],
            "bitmap_encoding": spec_info["bitmap_encoding"],
            "mti": doc_dec.get("t", "?"),
            "fields_present": fields_present,
            "fields_null": 130 - fields_present,
            "raw_length_bytes": len(raw_data),
        }
    }

    for key in ORDERED_KEYS:
        label = FIELD_MAP.get(key, f"de_{key}")
        value = doc_dec.get(key, None)
        if isinstance(value, (bytes, bytearray)):
            value = value.hex().upper()
        output[label] = value

    indent = 2 if pretty else None
    return json.dumps(output, indent=indent, ensure_ascii=False, default=str)


def parse_iso_to_dict(raw_data: Union[bytes, bytearray]) -> dict:
    """Même logique mais retourne un dict Python (pour Spark / FastAPI)."""
    spec_name = detect_spec(raw_data)
    spec = SPEC_REGISTRY[spec_name]
    doc_dec, _ = iso8583.decode(raw_data, spec)

    output = {"_spec_detected": spec_name}
    for key in ORDERED_KEYS:
        label = FIELD_MAP.get(key, f"de_{key}")
        value = doc_dec.get(key, None)
        if isinstance(value, (bytes, bytearray)):
            value = value.hex().upper()
        output[label] = value

    return output



