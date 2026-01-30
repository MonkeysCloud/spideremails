import asyncio
import logging
import pandas as pd
import os
import time
from vc_email_scraper import hunt_emails

# CONFIG
CHECKPOINT_CSV = "openvc_websites_checkpoint.csv"
OUTPUT_CSV = "openvc_emails.csv"

logging.basicConfig(
    format="%(asctime)s %(levelname)s %(message)s",
    datefmt="%H:%M:%S",
    level=logging.INFO,
)
log = logging.getLogger("email_hunter")

async def main():
    log.info("Starting continuous email hunter...")
    
    # Initialize output if not exists
    if not os.path.exists(OUTPUT_CSV):
        pd.DataFrame(columns=["website", "email"]).to_csv(OUTPUT_CSV, index=False)
    
    processed_sites = set()
    
    # Load initially processed and clean duplicates
    if os.path.exists(OUTPUT_CSV):
        try:
            df_out = pd.read_csv(OUTPUT_CSV)
            # Deduplicate
            initial_len = len(df_out)
            df_out.drop_duplicates(subset=["website", "email"], inplace=True)
            if len(df_out) < initial_len:
                log.info(f"Removed {initial_len - len(df_out)} duplicate rows from {OUTPUT_CSV}")
                df_out.to_csv(OUTPUT_CSV, index=False)
            
            if "website" in df_out.columns:
                processed_sites = set(df_out["website"].unique())
        except Exception as e:
            log.error(f"Error reading/cleaning CSV: {e}")
            pass

    while True:
        if not os.path.exists(CHECKPOINT_CSV):
            log.info("Waiting for checkpoint file...")
            await asyncio.sleep(5)
            continue
            
        try:
            # Load found websites
            df_in = pd.read_csv(CHECKPOINT_CSV)
            if "website" not in df_in.columns:
                await asyncio.sleep(5)
                continue
                
            current_sites = set(df_in["website"].unique())
            
            # Identify new sites
            # We exclude sites that are already in the output CSV (processed_sites)
            # This implicitly retries sites that yielded 0 emails (were not saved)
            new_sites = list(current_sites - processed_sites)
            
            if not new_sites:
                log.info("No new sites to process. Waiting...")
                await asyncio.sleep(10)
                continue
                
            log.info(f"Found {len(new_sites)} new sites to hunt!")
            
            # Run email hunter
            results = await hunt_emails(new_sites)
            
            # Save results (append)
            if results:
                new_rows = pd.DataFrame(results, columns=["website", "email"])
                # Filter out empty emails
                new_rows = new_rows[new_rows["email"] != ""]
                # Drop duplicates within the batch
                new_rows.drop_duplicates(inplace=True)
                
                if not new_rows.empty:
                    # Append header only if file doesn't exist
                    header = not os.path.exists(OUTPUT_CSV)
                    new_rows.to_csv(OUTPUT_CSV, mode='a', header=header, index=False)
                    log.info(f"✔ Added {len(new_rows)} emails to {OUTPUT_CSV}")
                    
                    # Update processed list with successful sites
                    successful_sites = set(new_rows["website"].unique())
                    processed_sites.update(successful_sites)
                
                # NOTE: If a site yielded 0 emails, it's NOT added to processed_sites here.
                # It will be retried on next loop iteration? 
                # Yes, current_sites - processed_sites.
                # If we want to avoid infinite retries of 0-email sites, we should add them to processed_sites.
                # BUT, for now, we want retries because of the User-Agent fix.
                # However, infinite retries in a tight loop is bad.
                # We should track "attempted" sites in memory for this session.
                
                processed_sites.update(set(new_sites)) # Mark ALL accepted new_sites as processed for this session
            
            else:
                 # No results found for this batch
                 processed_sites.update(set(new_sites))
            
        except Exception as e:
            log.error(f"Error in hunting loop: {e}")
            await asyncio.sleep(5)

if __name__ == "__main__":
    asyncio.run(main())
