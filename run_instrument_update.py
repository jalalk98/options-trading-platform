from backend.services.instrument_registry import extract_instrument_file
import logging



logging.basicConfig(level=logging.INFO)

print("Starting weekly instrument update...")


extract_instrument_file()


print("Instrument update completed.")
