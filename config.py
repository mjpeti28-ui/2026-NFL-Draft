import os
from dotenv import load_dotenv

load_dotenv(override=True)  # override=True ensures .env values win over parent process env vars

def _require(key: str) -> str:
    val = os.environ.get(key)
    if not val:
        raise EnvironmentError(
            f"Missing required environment variable: {key}\n"
            f"Copy .env.example to .env and fill in your API keys."
        )
    return val

ANTHROPIC_API_KEY = _require("ANTHROPIC_API_KEY")
AIRTABLE_PAT = _require("AIRTABLE_PAT")
AIRTABLE_BASE_ID = os.environ.get("AIRTABLE_BASE_ID", "appQSFIydAE6Eo5PR")

CLAUDE_MODEL = "claude-sonnet-4-6"

# Airtable table IDs
TABLE_PLAYER   = "tblUt4CGSie2fFkyM"
TABLE_SOURCE   = "tblYstoeXCP2CslcP"
TABLE_ARTIFACT = "tblEgLR5tYCmgeNPp"
TABLE_CLAIM    = "tbltQZOK3gwBkQGTi"

# Player field IDs
F_PLAYER_FIRST    = "fldJFw41GdshpeAZa"
F_PLAYER_LAST     = "fldE6RSh56zi7FkDv"
F_PLAYER_POSITION = "fldNizj0csjDdaKxL"
F_PLAYER_SCHOOL   = "fldLHhNHAE2zflDTB"
F_PLAYER_KEY      = "fldGVlHdAMoWE28y8"

# Source field IDs
F_SOURCE_NAME     = "fldhzI5h7LHfh6XNB"
F_SOURCE_PLATFORM = "fldMFrHXtFKntEO4E"
F_SOURCE_CHANNEL  = "fldyNMsBok6EuT507"
F_SOURCE_URL      = "fld4FijxvEdZsJgCs"

# Artifact field IDs
F_ARTIFACT_TITLE    = "fld21upg1hPm3awc3"
F_ARTIFACT_SOURCE   = "fldHeuGXgJXuMdAUi"
F_ARTIFACT_TYPE     = "fldoFdIMb3EVWkiTx"
F_ARTIFACT_URL      = "fld76VcyrPVOePvml"
F_ARTIFACT_DATE     = "fldqZrEYuK8SBJKPz"
F_ARTIFACT_CONTEXT  = "fldAvFbDIHmPutJxN"
F_ARTIFACT_NOTES    = "fldrEx684B7qkqnYz"
F_ARTIFACT_CLAIMS   = "fldPGs56fBmJuNXmp"

# Claim field IDs
F_CLAIM_PLAYER   = "fldoiYg2LmOn3h6D4"
F_CLAIM_ARTIFACT = "flduQaJJqp2nZZPpg"
F_CLAIM_TYPE     = "fldd2X45iVbTtAWXi"
F_CLAIM_CATEGORY = "fldbq9nhXKlq75Xf9"
F_CLAIM_TEXT     = "fld0b34UKQe5gDTnd"

VALID_CLAIM_TYPES = [
    "Strength", "Weakness", "Projection", "Trait", "Scheme Fit",
    "Red Flag", "Comparison", "Production", "Context", "Grade",
    "Measurement", "Medical/Character", "Ranking", "Development",
]

VALID_CATEGORIES = [
    # Physical
    "Athleticism", "Speed / Burst", "Size / Measurements", "Strength / Power",
    "Agility / Quickness", "Combine Results",
    # Skill
    "Route Running", "Ball Skills / Hands", "Blocking", "Pass Rush",
    "Coverage", "Tackling", "Footwork", "Arm Talent", "Technique / Mechanics",
    # Mental
    "Football IQ", "Decision Making", "Leadership / Character",
    # Context
    "Draft Range", "Player Comparison", "Production / Stats",
    "Competition Level", "Scheme Fit", "Injury / Medical", "Background / Bio",
    "Age / Experience", "Overall Grade", "Projection Risk",
]

VALID_PLATFORMS = [
    "YouTube", "Podcast", "Article", "Substack", "Twitter/X", "TikTok", "TV", "Other"
]

VALID_ARTIFACT_TYPES = [
    "Video", "Podcast Episode", "Article", "Thread", "Other"
]
