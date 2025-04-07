"""
Pipeline module for QMT strategies
"""

def __getattr__(name):
    if name == 'MultiFactorPipeline':
        from .pipeline import MultiFactorPipeline
        return MultiFactorPipeline
    raise AttributeError(f"module '{__name__}' has no attribute '{name}'")

__all__ = ['MultiFactorPipeline'] 