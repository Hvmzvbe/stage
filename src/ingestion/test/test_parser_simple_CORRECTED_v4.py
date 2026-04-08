"""
TEST SIMPLE : Parser ISO 8583 v4.0.1
Hamza — Al Barid Bank
CORRIGÉ pour pyiso8583 v4.0.1 (pas de specs)
"""

import sys
from pathlib import Path

# Ajoute le dossier parent au path
sys.path.insert(0, str(Path(__file__).parent.parent))

import pyiso8583
from iso8583_parser import UniversalISO8583Parser

print("=" * 80)
print("TEST SIMPLE : Parser ISO 8583 → JSON")
print("=" * 80)

# ============================================================================
# ÉTAPE 1 : Crée un message ISO 8583
# ============================================================================
print("\n📥 **ÉTAPE 1 : Crée un message ISO 8583**\n")

doc = {
    "t": "0100",                           # Message Type Indicator
    "2": "4911111111111111",               # PAN (Primary Account Number)
    "3": "000000",                         # Processing Code
    "4": "000000050000",                   # Amount (500.00 MAD)
    "7": "0408143025",                     # Transmission DateTime
    "11": "123456",                        # STAN (System Trace Audit Number)
    "12": "143025",                        # Local Time
    "13": "0408",                          # Local Date
    "18": "5812",                          # MCC (Merchant Category Code)
    "22": "051",                           # POS Entry Mode
    "37": "RRN001",                        # Retrieval Reference Number
    "41": "TRM00001",                      # Terminal ID
    "43": "MCDONALD CASABLANCA  MA",       # Merchant Location
    "49": "504",                           # Currency Code (MAD)
}

print("Champs ISO 8583 entrés :")
for k, v in doc.items():
    print(f"  {k:3} : {v}")

# ============================================================================
# ÉTAPE 2 : Encode en trame ISO 8583 (binaire)
# ============================================================================
print("\n📊 **ÉTAPE 2 : Encode en trame ISO 8583 (binaire)**\n")

try:
    # ✅ En v4.0.1 : pas de spec, juste encode(doc)
    raw_frame = pyiso8583.encode(doc)
    
    print(f"✅ Encoding réussi !")
    print(f"   Longueur : {len(raw_frame)} bytes")
    print(f"   Hex (50 premiers bytes) : {raw_frame[:50].hex()}")
    
except Exception as e:
    print(f"❌ Erreur encoding : {type(e).__name__}")
    print(f"   Message : {e}")
    import traceback
    traceback.print_exc()
    exit(1)

# ============================================================================
# ÉTAPE 3 : Parse la trame avec notre UniversalISO8583Parser
# ============================================================================
print("\n🔍 **ÉTAPE 3 : Parse la trame avec UniversalISO8583Parser**\n")

try:
    parser = UniversalISO8583Parser()
    parsed = parser.parse_frame(raw_frame)
    
    print(f"✅ Parsing réussi !")
    print(f"   MTI : {parsed['MTI']}")
    print(f"   Description : {parsed['MTI_description']}")
    print(f"   Type de transaction : {parsed['transaction_type']}")
    
except Exception as e:
    print(f"❌ Erreur parsing : {type(e).__name__}")
    print(f"   Message : {e}")
    import traceback
    traceback.print_exc()
    exit(1)

# ============================================================================
# ÉTAPE 4 : Affiche les fraud features
# ============================================================================
print("\n🚨 **ÉTAPE 4 : Features Anti-Fraude Détectées**\n")

fraud_features = parsed.get("fraud_features", {})
print("Fraud Features :")
for key, value in fraud_features.items():
    if value is not None:
        print(f"  {key:30} : {value}")

# ============================================================================
# ÉTAPE 5 : Affiche le JSON structuré
# ============================================================================
print("\n📋 **ÉTAPE 5 : JSON Structuré (complet)**\n")

import json
print(json.dumps(parsed, indent=2, default=str))

print("\n" + "=" * 80)
print("✅ TEST RÉUSSI !")
print("=" * 80)
