Get-Process python -ErrorAction SilentlyContinue | Stop-Process -Force
Start-Sleep -Seconds 1

Write-Host "--- Async mode: expect data loss ---"
$p = Start-Process python -ArgumentList "-m miniredis.server --role primary --port 6380 --grpc-port 7380 --node-id node-a --replica-of 127.0.0.1:7381 --peer node-b,127.0.0.1:7381,127.0.0.1:6381 --write-mode async" -NoNewWindow -PassThru
Start-Process python -ArgumentList "-m miniredis.server --role replica --port 6381 --grpc-port 7381 --node-id node-b --primary-address 127.0.0.1:7380 --primary-node-id node-a --peer node-a,127.0.0.1:7380,127.0.0.1:6380" -NoNewWindow -PassThru
Start-Sleep -Seconds 2
"SET k1 v1" | python -m miniredis.cli_client --port 6380
Stop-Process -Id $p.Id -Force
"GET k1" | python -m miniredis.cli_client --port 6381
Get-Process python -ErrorAction SilentlyContinue | Stop-Process -Force
Start-Sleep -Seconds 1

Write-Host "--- Quorum mode: expect no data loss ---"
$p = Start-Process python -ArgumentList "-m miniredis.server --role primary --port 6380 --grpc-port 7380 --node-id node-a --replica-of 127.0.0.1:7381 --peer node-b,127.0.0.1:7381,127.0.0.1:6381 --write-mode quorum" -NoNewWindow -PassThru
Start-Process python -ArgumentList "-m miniredis.server --role replica --port 6381 --grpc-port 7381 --node-id node-b --primary-address 127.0.0.1:7380 --primary-node-id node-a --peer node-a,127.0.0.1:7380,127.0.0.1:6380" -NoNewWindow -PassThru
Start-Sleep -Seconds 2
"SET k1 v1" | python -m miniredis.cli_client --port 6380
Stop-Process -Id $p.Id -Force
"GET k1" | python -m miniredis.cli_client --port 6381
Get-Process python -ErrorAction SilentlyContinue | Stop-Process -Force
Start-Sleep -Seconds 1

Write-Host "--- Split-brain ---"
pytest tests/test_split_brain.py -v -s