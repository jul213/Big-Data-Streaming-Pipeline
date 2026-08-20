from fastapi.testclient import TestClient
from src.api import app

client = TestClient(app)

def test_health_endpoint():
    response = client.get("/health")
    assert response.status_code == 200
    data = response.json()
    assert data["status"] == "healthy"
    assert data["engine"] == "BigData-Stream-Processor"

def test_ingest_endpoint_flow():
    # Enviar 5 eventos para completar un lote y verificar la respuesta analítica
    for i in range(5):
        payload = {
            "event_id": f"test-evt-{i}",
            "source_ip": "10.0.0.99",
            "action": "STREAM_TEST",
            "payload_size_bytes": 512
        }
        response = client.post("/ingest", json=payload)
        assert response.status_code == 202
        
        data = response.json()
        if i == 4:
            # El quinto evento debe activar el lote
            assert data["status"] == "batch_processed"
            assert data["analytics"]["batch_records"] == 5
            assert "action_distribution" in data["analytics"]
        else:
            assert data["status"] == "buffered"