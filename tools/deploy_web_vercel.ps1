param(
    [string]$ApiBaseUrl
)

$ErrorActionPreference = "Stop"

if (-not $ApiBaseUrl) {
    throw "Provide -ApiBaseUrl, for example https://aero-pulse-api-xxxxx.a.run.app"
}

Write-Host "Set these Vercel environment variables for apps/web:"
Write-Host "API_BASE_URL=$ApiBaseUrl"
Write-Host "NEXT_PUBLIC_API_BASE_URL=$ApiBaseUrl"
Write-Host ""
Write-Host "Then deploy with Root Directory = apps/web."
Write-Host "Example:"
Write-Host "  vercel --prod"
