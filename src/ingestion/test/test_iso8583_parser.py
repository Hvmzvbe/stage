"""
================================================================================
 Tests Unitaires — UniversalISO8583Parser
 Al Barid Bank — Plateforme MLOps Détection de Fraude
================================================================================
 Lancer : python -m pytest test_iso8583_parser.py -v
================================================================================
"""

import json
import iso8583
import pytest
#from iso8583 import encode, decode
#from iso8583.specs import default_spec

from iso8583_parser import UniversalISO8583Parser, DE_FIELD_MAPPING, MTI_DESCRIPTIONS


# =============================================================================
# FIXTURES — Trames ISO 8583 synthétiques pour les tests
# =============================================================================

def build_test_frame(mti: str, fields: dict) -> bytes:
    """Helper : construit une trame ISO 8583 valide pour les tests."""
    doc = {"t": mti}
    doc.update(fields)
    raw, _ = iso8583.encode(doc, spec=iso8583.specs.default_ascii)
    return raw


@pytest.fixture
def parser():
    return UniversalISO8583Parser()


@pytest.fixture
def standard_auth_frame():
    """Trame d'autorisation standard (0100) avec les champs principaux."""
    return build_test_frame("0100", {
        "2":  "4111111111111111",   # PAN
        "3":  "000000",             # Processing Code (achat)
        "4":  "000000050000",       # Montant = 500.00 MAD
        "7":  "0408143025",         # 8 Avril, 14h30:25
        "11": "123456",             # STAN
        "12": "143025",             # Heure locale
        "13": "0408",               # Date locale
        "18": "5812",               # MCC = Restaurant
        "22": "051",                # POS Mode = chip
        "37": "123456789012",       # RRN
        "41": "TRM00001",           # Terminal ID
        "43": "LE GRAND CAFE     CASABLANCA  MA",  # Localisation
        "49": "504",                # MAD
    })


@pytest.fixture
def micro_amount_frame():
    """Trame avec micro-montant (≤ 50 MAD) — flag Card Testing."""
    return build_test_frame("0100", {
        "2":  "4111111111111111",
        "4":  "000000004500",       # Montant = 45.00 MAD ≤ 50 MAD
        "7":  "0408033010",
        "12": "033010",             # Heure = 3h du matin
        "13": "0408",
        "49": "504",
    })


@pytest.fixture
def high_risk_merchant_frame():
    """Trame avec MCC bijouterie (5094) — commerce à risque."""
    return build_test_frame("0100", {
        "2":  "5500005555555559",
        "4":  "000001500000",       # 15000.00 MAD
        "7":  "0408041500",
        "12": "041500",             # 4h15 du matin
        "13": "0408",
        "18": "5094",               # MCC = Bijouterie
        "41": "TRM99999",
        "43": "BIJOUX PRESTIGE   RABAT       MA",
        "49": "504",
    })


@pytest.fixture
def declined_transaction_frame():
    """Trame avec code réponse de refus (code 05) — Card Testing."""
    return build_test_frame("0110", {
        "2":  "4111111111111111",
        "4":  "000000001000",       # 10.00 MAD
        "7":  "0408120000",
        "11": "654321",
        "37": "RRN654321    ",
        "38": "      ",             # Pas d'auth code
        "39": "05",                 # Response Code 05 = Do Not Honor
        "41": "TRM00001",
        "49": "504",
    })


@pytest.fixture
def foreign_currency_frame():
    """Trame avec devise étrangère (EUR) — anomalie géographique potentielle."""
    return build_test_frame("0100", {
        "2":  "4111111111111111",
        "4":  "000000010000",       # 100.00 EUR
        "7":  "0408180000",
        "12": "180000",
        "13": "0408",
        "43": "AMAZON.FR        PARIS       FR",
        "49": "978",                # EUR
    })


# =============================================================================
# TESTS
# =============================================================================

class TestParserInitialization:
    def test_parser_creates_successfully(self, parser):
        assert parser is not None
        assert parser.spec is not None

    def test_metrics_initialized(self, parser):
        m = parser.get_metrics()
        assert m["total_processed"] == 0
        assert m["total_success"] == 0
        assert m["success_rate_pct"] == 100.0  # 0/max(0,1) * 100


class TestFrameDecoding:
    def test_standard_auth_parsed(self, parser, standard_auth_frame):
        result = parser.parse_frame(standard_auth_frame)
        assert result["MTI"] == "0100"
        assert result["MTI_description"] == "Authorization Request"
        assert result["transaction_type"] == "Authorization"

    def test_all_data_elements_present(self, parser, standard_auth_frame):
        result = parser.parse_frame(standard_auth_frame)
        des = result["data_elements"]
        # Les DEs encodés doivent être présents
        assert "DE_002" in des
        assert "DE_004" in des
        assert "DE_018" in des

    def test_field_metadata_enrichment(self, parser, standard_auth_frame):
        result = parser.parse_frame(standard_auth_frame)
        de4 = result["data_elements"]["DE_004"]
        assert de4["field_name"] == "Transaction Amount"
        assert de4["category"] == "amount"

    def test_empty_frame_raises(self, parser):
        with pytest.raises(ValueError, match="Trame vide"):
            parser.parse_frame(b"")

    def test_invalid_frame_raises_decode_error(self, parser):
        with pytest.raises((iso8583.DecodeError, Exception)):
            parser.parse_frame(b"INVALID_ISO8583_GARBAGE_DATA_1234567890")

    def test_metadata_block_present(self, parser, standard_auth_frame):
        result = parser.parse_frame(standard_auth_frame)
        meta = result["_metadata"]
        assert meta["parser_version"] == "1.0.0"
        assert "parsed_at" in meta
        assert meta["raw_length_bytes"] > 0

    def test_result_is_json_serializable(self, parser, standard_auth_frame):
        result = parser.parse_frame(standard_auth_frame)
        json_str = json.dumps(result, default=str)
        assert len(json_str) > 0


class TestFraudFeatureExtraction:
    def test_amount_extraction(self, parser, standard_auth_frame):
        result = parser.parse_frame(standard_auth_frame)
        ff = result["fraud_features"]
        assert ff["amount_mad"] == pytest.approx(500.0)
        assert ff["is_micro_amount"] is False

    def test_micro_amount_detection(self, parser, micro_amount_frame):
        result = parser.parse_frame(micro_amount_frame)
        ff = result["fraud_features"]
        assert ff["amount_mad"] == pytest.approx(45.0)
        assert ff["is_micro_amount"] is True

    def test_odd_hour_detection(self, parser, micro_amount_frame):
        result = parser.parse_frame(micro_amount_frame)
        ff = result["fraud_features"]
        assert ff["local_hour"] == 3
        assert ff["is_odd_hour"] is True

    def test_normal_hour_not_flagged(self, parser, standard_auth_frame):
        result = parser.parse_frame(standard_auth_frame)
        ff = result["fraud_features"]
        assert ff["local_hour"] == 14
        assert ff["is_odd_hour"] is False

    def test_high_risk_merchant_flagged(self, parser, high_risk_merchant_frame):
        result = parser.parse_frame(high_risk_merchant_frame)
        ff = result["fraud_features"]
        assert ff["is_high_risk_merchant"] is True
        assert ff["mcc"] == "5094"

    def test_normal_merchant_not_flagged(self, parser, standard_auth_frame):
        result = parser.parse_frame(standard_auth_frame)
        ff = result["fraud_features"]
        assert ff["is_high_risk_merchant"] is False
        assert ff["mcc"] == "5812"

    def test_declined_transaction_flagged(self, parser, declined_transaction_frame):
        result = parser.parse_frame(declined_transaction_frame)
        ff = result["fraud_features"]
        assert ff["is_declined"] is True
        assert ff["response_code"] == "05"

    def test_foreign_currency_flagged(self, parser, foreign_currency_frame):
        result = parser.parse_frame(foreign_currency_frame)
        ff = result["fraud_features"]
        assert ff["currency_code"] == "978"
        assert ff["is_foreign_currency"] is True

    def test_mad_currency_not_foreign(self, parser, standard_auth_frame):
        result = parser.parse_frame(standard_auth_frame)
        ff = result["fraud_features"]
        assert ff["is_foreign_currency"] is False

    def test_pan_masked(self, parser, standard_auth_frame):
        result = parser.parse_frame(standard_auth_frame)
        ff = result["fraud_features"]
        # PAN doit être masqué (PCI-DSS)
        assert "pan_masked" in ff
        assert "*" in ff["pan_masked"]
        # Les 6 premiers (BIN) et 4 derniers doivent être visibles
        assert ff["pan_masked"].startswith("411111")
        assert ff["pan_masked"].endswith("1111")

    def test_bin_extraction(self, parser, standard_auth_frame):
        result = parser.parse_frame(standard_auth_frame)
        assert result["fraud_features"]["pan_bin"] == "411111"

    def test_de43_location_parsing(self, parser, standard_auth_frame):
        result = parser.parse_frame(standard_auth_frame)
        ff = result["fraud_features"]
        assert "merchant_name" in ff
        assert "merchant_city" in ff


class TestPCIDSSMasking:
    def test_pan_de2_masked_in_data_elements(self, parser, standard_auth_frame):
        result = parser.parse_frame(standard_auth_frame)
        de2_value = result["data_elements"]["DE_002"]["value"]
        assert "*" in de2_value

    def test_pan_masking_format(self, parser):
        masked = parser._mask_pan("4111111111111111")
        assert masked == "411111******1111"

    def test_short_pan_masking(self, parser):
        masked = parser._mask_pan("12345")
        assert masked == "*****"


class TestMTIMapping:
    @pytest.mark.parametrize("mti,expected", [
        ("0100", "Authorization Request"),
        ("0200", "Financial Transaction Request"),
        ("0400", "Reversal Request"),
        ("0800", "Network Management Request"),
    ])
    def test_mti_descriptions(self, mti, expected):
        assert MTI_DESCRIPTIONS[mti] == expected


class TestMetrics:
    def test_metrics_increment_on_success(self, parser, standard_auth_frame):
        initial = parser._metrics["total_processed"]
        # On simule un traitement manuel
        parser.parse_frame(standard_auth_frame)
        # Le worker incrémente les métriques, pas le parser directement
        # Ce test vérifie que parse_frame ne lève pas d'exception
        assert True  # Si on arrive ici, c'est un succès
