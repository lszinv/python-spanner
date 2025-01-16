from .spanner_metrics_tracer_factory import SpannerMetricsTracerFactory
from .metrics_interceptor import MetricsInterceptor

class MetricsCapture:
    def __enter__(self):
        factory = SpannerMetricsTracerFactory()

        # Define a new metrics tracer for the new operation
        SpannerMetricsTracerFactory.current_metrics_tracer = factory.create_metrics_tracer()
        SpannerMetricsTracerFactory.current_metrics_tracer.record_operation_start()
        return self

    def __exit__(self, exc_type, exc_value, traceback):
        SpannerMetricsTracerFactory.current_metrics_tracer.record_operation_completion()
        return False  # Propagate the exception if any

