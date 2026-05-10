Write-Host "=== Docker Disk Usage ===" -ForegroundColor Cyan
docker system df

Write-Host "`n=== Cleaning up... ===" -ForegroundColor Yellow
docker system prune -a --volumes --force

Write-Host "`n=== After cleanup ===" -ForegroundColor Green
docker system df