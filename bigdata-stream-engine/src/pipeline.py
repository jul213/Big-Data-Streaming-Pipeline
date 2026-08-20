import time
import random
import logging
from typing import List, Dict, Any
import pandas as pd

logging.basicConfig(level=logging.INFO, format="%(asctime)s - [BIGDATA-ENGINE] - %(levelname)s - %(message)s")
logger = logging.getLogger("BigDataEngine")

class StreamProcessor:
    def __init__(self, batch_size: int = 10):
        self.batch_size = batch_size
        self.buffer: List[Dict[str, Any]] = []

    def ingest_event(self, event: Dict[str, Any]) -> Dict[str, Any] | None:
        """
        Ingresa un evento al flujo en streaming. Si el buffer alcanza
        el tamaño del lote (batch_size), ejecuta la analítica columnar.
        """
        self.buffer.append(event)
        logger.info(f"Evento recibido en el stream. Buffer actual: {len(self.buffer)}/{self.batch_size}")
        
        if len(self.buffer) >= self.batch_size:
            return self.process_batch()
        return None

    def process_batch(self) -> Dict[str, Any]:
        """
        Procesa el lote actual utilizando Pandas para analítica de datos en memoria,
        simulando operaciones de Big Data (agregaciones, filtrado y métricas).
        """
        logger.info(">>> Procesando lote masivo (Batch Window Triggered)...")
        start_time = time.time()
        
        # Convertir buffer a DataFrame columnar
        df = pd.DataFrame(self.buffer)
        
        # Analítica simulada (Big Data Aggregations)
        total_events = len(df)
        unique_sources = df['source_ip'].nunique() if 'source_ip' in df else 0
        action_counts = df['action'].value_counts().to_dict() if 'action' in df else {}
        
        # Métricas de rendimiento de procesamiento
        processing_time = time.time() - start_time
        
        analytical_report = {
            "batch_records": total_events,
            "unique_sources": int(unique_sources),
            "distribution_actions": action_counts,
            "processing_latency_seconds": round(processing_time, 4),
            "status": "aggregated_successfully"
        }
        
        # Vaciar buffer para la siguiente ventana de streaming
        self.buffer.clear()
        logger.info(f"<<< Lote procesado con éxito en {processing_time:.4f}s")
        
        return analytical_report

# Simulador de generación de flujo masivo para pruebas técnicas
if __name__ == "__main__":
    processor = StreamProcessor(batch_size=5)
    mock_actions = ["DATA_ACCESS", "STREAM_READ", "WRITE_OP", "ANOMALY_DETECTED"]
    
    logger.info("Iniciando simulación de flujo de datos de alta velocidad...")
    for i in range(12):
        simulated_event = {
            "event_id": f"evt-{i+1:03d}",
            "source_ip": f"10.0.0.{random.randint(1, 50)}",
            "action": random.choice(mock_actions),
            "payload_size_bytes": random.randint(512, 16384),
            "timestamp": time.time()
        }
        result = processor.ingest_event(simulated_event)
        if result:
            print("\n--- INFORME ANALÍTICO DE STREAMING ---")
            print(result)
            print("---------------------------------------\n")
        time.sleep(0.2)