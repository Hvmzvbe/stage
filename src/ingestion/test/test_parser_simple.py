"""
================================================================================
 TEST SIMPLE — Parser ISO 8583 → JSON
 Convertit un message ISO 8583 en JSON structuré
================================================================================
 Utilisation : python test_parser_simple.py
================================================================================
"""

import json
import sys
from pathlib import Path

sys.path.insert(0, str(Path(__file__).parent))
sys.path.insert(0, str(Path(__file__).parent.parent))

import iso8583

#from pyiso8583.specs import default_spec 
from iso8583_parser import UniversalISO8583Parser


def test_simple():
    """Test simple : crée un message ISO → parse → affiche JSON."""
    
    print("\n" + "=" * 100)
    print("  TEST SIMPLE : Parser ISO 8583 → JSON")
    print("=" * 100 + "\n")
    
    # ════════════════════════════════════════════════════════════════════════
    # 1. CRÉE UN MESSAGE ISO 8583
    # ════════════════════════════════════════════════════════════════════════
    print("📥 **ÉTAPE 1 : Crée un message ISO 8583**\n")
    
    doc = {
        "t": "0100",                        # MTI = Authorization Request
        "2": "4911111111111111",            # PAN (carte VISA)
        "3": "000000",                      # Processing Code
        "4": "000000050000",                # Montant = 500.00 MAD
        "7": "0408143025",                  # Date/Heure
        "11": "123456",                     # STAN
        "12": "143025",                     # Local Time
        "13": "0408",                       # Local Date
        "18": "5812",                       # MCC (Restaurant)
        "22": "051",                        # POS Entry Mode
        "37": "RRN001",                     # RRN
        "41": "TRM00001",                   # Terminal ID
        "43": "MCDONALD CASABLANCA  MA",    # Merchant Location
        "49": "504",                        # Currency (MAD)
    }
    
    print("Champs ISO 8583 entrés :")
    for key, value in doc.items():
        print(f"  {key:3s} : {value}")
    
    # ════════════════════════════════════════════════════════════════════════
    # 2. ENCODE EN TRAME BINAIRE
    # ════════════════════════════════════════════════════════════════════════
    print("\n\n📊 **ÉTAPE 2 : Encode en trame ISO 8583 (binaire)**\n")
    
    raw_frame = iso8583.encode(doc,spec=iso8583.specs.default_spec)
    
    print(f"✓ Trame créée : {len(raw_frame)} bytes")
    print(f"  Hex : {raw_frame.hex()[:100]}...")
    print(f"  Raw : {raw_frame[:50]}")
    
    # ════════════════════════════════════════════════════════════════════════
    # 3. PARSE AVEC LE PARSER
    # ════════════════════════════════════════════════════════════════════════
    print("\n\n⚙️  **ÉTAPE 3 : Parse la trame avec UniversalISO8583Parser**\n")
    
    parser = UniversalISO8583Parser()
    result = parser.parse_frame(raw_frame)
    
    print("✓ Parsing réussi !\n")
    
    # ════════════════════════════════════════════════════════════════════════
    # 4. AFFICHE LE RÉSULTAT JSON
    # ════════════════════════════════════════════════════════════════════════
    print("📤 **ÉTAPE 4 : Résultat JSON (sortie du parser)**\n")
    
    print(json.dumps(result, indent=2, ensure_ascii=False))
    
    # ════════════════════════════════════════════════════════════════════════
    # 5. RÉSUMÉ
    # ════════════════════════════════════════════════════════════════════════
    print("\n\n" + "=" * 100)
    print("  RÉSUMÉ")
    print("=" * 100 + "\n")
    
    print("✓ **CONVERSION RÉUSSIE :**\n")
    print(f"  MTI           : {result['MTI']}")
    print(f"  Description   : {result['MTI_description']}")
    print(f"  Type          : {result['transaction_type']}")
    print(f"  DEs actifs    : {result['active_de_count']} champs")
    print(f"  Montant       : {result['fraud_features']['amount_mad']} MAD")
    print(f"  PAN (masqué)  : {result['fraud_features']['pan_masked']}")
    print(f"  Devise        : {result['fraud_features']['currency_code']}")
    print(f"  Commerce      : {result['fraud_features']['merchant_name']}")
    print(f"  Ville         : {result['fraud_features']['merchant_city']}")
    print(f"  Terminal      : {result['fraud_features']['terminal_id']}")
    
    print("\n✓ Le parser fonctionne correctement !\n")
    print("=" * 100 + "\n")


if __name__ == "__main__":
    test_simple()
