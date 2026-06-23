from pkgutil import extend_path

# Keep ops tests package-safe without shadowing top-level repo ops modules.
__path__ = extend_path(__path__, __name__)
