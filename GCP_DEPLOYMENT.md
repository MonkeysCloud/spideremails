# Running OpenVC Spider on Google Cloud

## 1. Create a GCP VM

```bash
gcloud compute instances create openvc-spider \
    --zone=us-central1-a \
    --machine-type=e2-medium \
    --image-family=ubuntu-2204-lts \
    --image-project=ubuntu-os-cloud \
    --boot-disk-size=20GB
```

## 2. Open Firewall (for VNC via SSH tunnel - no public port needed)

Only SSH is needed. VNC will be tunneled.

## 3. Connect and Setup

```bash
# SSH into VM
gcloud compute ssh openvc-spider --zone=us-central1-a

# Upload and run setup script
# (from your local machine, in the spiderwebsites directory)
gcloud compute scp gcp_setup.sh openvc-spider:~/ --zone=us-central1-a
gcloud compute ssh openvc-spider --zone=us-central1-a -- "chmod +x ~/gcp_setup.sh && ~/gcp_setup.sh"
```

## 4. Upload Spider Files

```bash
# Create directory on VM
gcloud compute ssh openvc-spider --zone=us-central1-a -- "mkdir -p ~/spiderwebsites/logos"

# Upload files
gcloud compute scp openvc_detailed_spider.py openvc-spider:~/spiderwebsites/ --zone=us-central1-a
gcloud compute scp openvc_detailed_results.csv openvc-spider:~/spiderwebsites/ --zone=us-central1-a
```

## 5. Run the Spider

```bash
# Terminal 1: SSH with VNC tunnel
gcloud compute ssh openvc-spider --zone=us-central1-a -- -L 5900:localhost:5900

# On VM: Start VNC and Chrome
~/start_vnc.sh
~/start_chrome.sh
```

Connect with a VNC client (like RealVNC or TigerVNC) to `localhost:5900`, password: `spider`

You'll see Chrome. Solve any CAPTCHAs there.

```bash
# Terminal 2: Run spider
gcloud compute ssh openvc-spider --zone=us-central1-a
~/run_spider.sh
```

## 6. Download Results

```bash
gcloud compute scp openvc-spider:~/spiderwebsites/openvc_detailed_results.csv ./ --zone=us-central1-a
gcloud compute scp --recurse openvc-spider:~/spiderwebsites/logos ./logos_gcp --zone=us-central1-a
```

## Tips

- **If IP gets blocked**: Stop and delete VM, create new one (new IP)
- **Cost**: ~$25/month if running 24/7, or ~$0.03/hour
- **Stop when not using**: `gcloud compute instances stop openvc-spider --zone=us-central1-a`
