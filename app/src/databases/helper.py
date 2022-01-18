import json



def format_text(elements: str, split_word: str = 'NEWHIT'):
    """
    Function to format raw data text before data cleaning, help json data conversion
    
    :return: a formatted string  
    """
    if not elements:
        return

    formatted_text = []

    try:
        for element in elements:
            if not element:
                continue 
            for entry in element.split(split_word)[1:]:
                if entry:
                    entry = entry.replace('\\"', '&quot;').replace("'", '')
                    entry = entry.replace('\\n', '').replace('\\', '')
                    try:
                        json_entry = json.loads(entry)
                        json_entry = {k: v.replace('\\\\', '\\') for k, v in json_entry.items()}
                        formatted_text.append(json_entry)
                    except ValueError as e1:
                        continue 
    except ValueError as e2:
        logging.info('Text formatting failed')
        return elements

    return formatted_text