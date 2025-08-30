import re
import numbers
import logging
from .logger import get_logger

logger = get_logger()


def apply_template(template, item):
    """Apply template with placeholders to item data."""

    try:

        placeholders = re.findall(r"\{(\w+)\}", template)

        for ph in placeholders:

            value = item.get(ph, None)

            if isinstance(value, (dict, list)):

                replacement = value  # Keep as dict or list

            elif value is None:

                # Replace None with 'null' for JSON compatibility
                replacement = "null"

            elif isinstance(value, numbers.Number):

                # Preserve original data types (int, float, Decimal)
                replacement = str(value)

            else:

                # Preserve strings
                replacement = value.replace('"', '\\"')

            if isinstance(replacement, str) and replacement != "null":

                # Ensure strings are properly quoted
                template = template.replace(f"{{{ph}}}", replacement)

            elif replacement == "null":

                template = template.replace(f"{{{ph}}}", replacement)

            else:

                # For numbers and booleans, convert to string without quotes
                template = template.replace(f"{{{ph}}}", replacement)

        return template

    except Exception as e:

        logger.error(f"Template Processing Error: {e}")
        return ""
