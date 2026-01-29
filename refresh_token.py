# refresh_token.py - Run this ONCE locally to get your refresh token

import msal

# Replace with your actual Azure app Client ID
CLIENT_ID = "d1adc91d-1026-4c7b-8ce9-ddc051dced98"

# IMPORTANT: Do NOT include offline_access here
SCOPES = [
    "https://graph.microsoft.com/Files.Read.All",
    "https://graph.microsoft.com/User.Read"
    # offline_access is added automatically by MSAL for interactive flow
]

# For personal Microsoft accounts (@outlook.com, @hotmail.com, etc.)
AUTHORITY = "https://login.microsoftonline.com/consumers"

app = msal.PublicClientApplication(
    client_id=CLIENT_ID,
    authority=AUTHORITY
)

# This opens your browser for login and consent
result = app.acquire_token_interactive(scopes=SCOPES)

if "access_token" in result:
    print("\nLogin successful!")
    print("\nYour long-lived REFRESH TOKEN (copy this carefully):")
    print("=" * 60)
    print(result["refresh_token"])
    print("=" * 60)
    print("\nStore this securely in GCP Secret Manager or as an environment variable.")
    print("Do NOT share it or commit it to code.")
elif "error" in result:
    print("Error:", result.get("error"))
    print("Description:", result.get("error_description"))
else:
    print("Unknown response:", result)