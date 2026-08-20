import time
import logging
from typing import List, Dict, Any, Optional
from fastapi import FastAPI, HTTPException, status
from pydantic import BaseModel, Field
import pandas as pd

# Configuración de Logging Estructurado (Estándar de Producción)
logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s - %(name)s - [%(levelname)s] - %(message)s"
)
logger = logging.getLogger("BigDataAPI")

# Inicialización de la aplicación FastAPI con metadatos profesionales
app = FastAPI(
    title="Enterprise Big Data Stream Ingestion Engine Julio",
    description="High-performance streaming telemetry ingestion and columnar analytics engine for 2026-2030 architectures test.",
    version="1.0.0"
)

# 1. Definición del Esquema de Datos Estricto (Pydantic v2)
class TelemetryEvent(BaseModel):
    event_id: str = Field(..., description="Identificador único del evento", example="evt-001")
    source_ip: str = Field(..., description="Dirección IP de origen del flujo", example="10.0.0.15")
    action: str = Field(..., description="Acción o tipo de operación ejecutada", example="DATA_ACCESS")
    payload_size_bytes: int = Field(..., gt=0, description="Tamaño del payload en bytes", example=1024)
    timestamp: Optional[float] = Field(default_factory=time.time, description="Marca de tiempo UNIX")

class BatchResponse(BaseModel):
    status: str
    message: str
    analytics: Optional[Dict[str, Any]] = None

# 2. Motor de Procesamiento en Streaming con Ventanas (Micro-batching)
class ProductionStreamProcessor:
    def __init__(self, batch_size: int = 10):
        self.batch_size = batch_size
        self.buffer: List[Dict[str, Any]] = []

    def add_event(self, event: Dict[str, Any]) -> Optional[Dict[str, Any]]:
        self.buffer.append(event)
        logger.info(f"Evento encolado. Estado del buffer: {len(self.buffer)}/{self.batch_size}")
        
        if len(self.buffer) >= self.batch_size:
            return self._compute_columnar_analytics()
        return None

    def _compute_columnar_analytics(self) -> Dict[str, Any]:
        """
        Ejecuta analítica columnar de alta velocidad utilizando Pandas en memoria.
        Simula agregaciones distribuidas típicas de sistemas Big Data.
        """
        logger.info(">>> Ventana de lote alcanzada. Ejecutando analítica columnar...")
        start_time = time.time()
        
        df = pd.DataFrame(self.buffer)
        
        total_records = int(len(df))
        unique_sources = int(df['source_ip'].nunique()) if 'source_ip' in df else 0
        action_distribution = df['action'].value_counts().to_dict() if 'action' in df else {}
        avg_payload = float(df['payload_size_bytes'].mean()) if 'payload_size_bytes' in df else 0.0
        
        latency = time.time() - start_time
        
        report = {
            "batch_records": total_records,
            "unique_sources": unique_sources,
            "action_distribution": action_distribution,
            "average_payload_bytes": round(avg_payload, 2),
            "processing_latency_seconds": round(latency, 4),
            "computed_at": time.time()
        }
        
        # Limpieza del buffer para la siguiente ventana
        self.buffer.clear()
        logger.info(f"<<< Analítica de lote completada en {latency:.4f}s")
        return report

# Instancia global del procesador de streaming (Ventana de 5 eventos para pruebas rápidas)
stream_engine = ProductionStreamProcessor(batch_size=5)

# 3. Endpoints de la API
@app.get("/health", status_code=status.HTTP_200_OK, tags=["System Health"])
def health_check():
    """Endpoint de verificación de salud para orquestadores (Kubernetes/Docker)."""
    return {
        "status": "healthy",
        "engine": "BigData-Stream-Processor",
        "current_buffer_size": len(stream_engine.buffer)
    }

@app.post("/ingest", response_model=BatchResponse, status_code=status.HTTP_202_ACCEPTED, tags=["Stream Ingestion"])
def ingest_telemetry(event: TelemetryEvent):
    """
    Ingresa un evento de telemetría individual al flujo en streaming.
    Cuando el búfer alcanza el tamaño del lote, devuelve el informe analítico consolidado.
    """
    try:
        event_dict = event.model_dump()
        batch_result = stream_engine.add_event(event_dict)
        
        if batch_result:
            return BatchResponse(
                status="batch_processed",
                message="El lote de eventos alcanzó el umbral y fue procesado exitosamente.",
                analytics=batch_result
            )
        
        return BatchResponse(
            status="buffered",
            message=f"Evento ingerido correctamente. Acumulados: {len(stream_engine.buffer)}/{stream_engine.batch_size}",
            analytics=None
        )
    except Exception as e:
        logger.error(f"Error crítico procesando el evento: {str(e)}")
        raise HTTPException(
            status_code=status.HTTP_500_INTERNAL_SERVER_ERROR,
            detail=f"Error interno en el pipeline de datos: {str(e)}"
        )