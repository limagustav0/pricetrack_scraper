from opentelemetry import trace
from opentelemetry.sdk.trace import TracerProvider
from opentelemetry.sdk.resources import Resource

from opentelemetry.sdk.trace.export import (
    BatchSpanProcessor,
)

from opentelemetry.exporter.otlp.proto.http.trace_exporter import OTLPSpanExporter

OTLP_ENDPOINT = "http://localhost:4318/v1/traces"

APP = "pricetrack-scraper"

# Use string literals directly - no imports needed
resource = Resource.create({
    "service.name": APP, 
    "service.version": "1.0.0"
})

provider = TracerProvider(resource=resource)
#processor = BatchSpanProcessor(ConsoleSpanExporter())
processor = BatchSpanProcessor(OTLPSpanExporter(endpoint=OTLP_ENDPOINT))
provider.add_span_processor(processor)
trace.set_tracer_provider(provider)
tracer = trace.get_tracer(APP)

