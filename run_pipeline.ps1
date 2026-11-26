if (!(Test-Path .env)) { Copy-Item .env .env; Write-Host 'Created .env. Edit PG_PASSWORD, then run this script again.'; exit 1 }
if (!(Test-Path .venv)) { py -3.11 -m venv .venv }
.\.venv\Scripts\Activate.ps1
pip install -r requirements.txt
python src/pipeline.py
