import pandas as pd
import glob
import os

TARGET_FILE = "openvc_emails.csv"
BLACKLIST_PREFIXES = ["hr", "privacy", "careers", "accessibility", "contact", "info", "hello", "support", "admin", "jobs", "inquiries", "press", "media", "team", "legal"]
# The user specifically mentioned: hr, privacy, careers, accessibility. 
# They also said "empty".
# I will stick to their specific list + "empty".
STRICT_BLACKLIST = ["hr", "privacy", "careers", "accessibility", "support", "legal"]

OTHER_FILES = [
    "openvc_emails.bak",
    "vc_emails.csv",
    "vc_emails_vcsheet.csv"
]

def clean_emails():
    print(f"Reading target file: {TARGET_FILE}")
    try:
        df = pd.read_csv(TARGET_FILE)
    except Exception as e:
        print(f"Error reading {TARGET_FILE}: {e}")
        return

    print(f"Initial row count: {len(df)}")

    # 1. Remove empty emails
    df = df.dropna(subset=["Email"])
    df = df[df["Email"].str.strip() != ""]
    print(f"After removing empty/NaN emails: {len(df)}")

    # 2. Remove blacklisted prefixes
    # Ensure lowercase for comparison
    def is_valid_email(email):
        email_lower = str(email).lower()
        for prefix in STRICT_BLACKLIST:
            if email_lower.startswith(prefix):
                return False
        return True

    df = df[df["Email"].apply(is_valid_email)]
    print(f"After removing blacklisted prefixes ({STRICT_BLACKLIST}): {len(df)}")

    # 3. Deduplicate against other files
    existing_emails = set()
    for fname in OTHER_FILES:
        if os.path.exists(fname):
            try:
                print(f"Reading reference file: {fname}")
                other_df = pd.read_csv(fname)
                # Assume column might be "Email" or "email"
                col_name = "Email" if "Email" in other_df.columns else "email"
                if col_name in other_df.columns:
                    emails = other_df[col_name].dropna().astype(str).str.lower().tolist()
                    existing_emails.update(emails)
            except Exception as e:
                print(f"Warning: Could not read {fname}: {e}")
    
    print(f"Found {len(existing_emails)} unique emails in other files.")

    # Filter out existing
    df = df[~df["Email"].str.lower().isin(existing_emails)]
    print(f"After removing duplicates from other files: {len(df)}")

    # Save
    df.to_csv(TARGET_FILE, index=False)
    print(f"Saved cleaned file to {TARGET_FILE}")

if __name__ == "__main__":
    clean_emails()
