import re

# Mapping of words to digits
word_to_digit = {
    'zero': '0', 'one': '1', 'two': '2', 'three': '3', 'four': '4',
    'five': '5', 'six': '6', 'seven': '7', 'eight': '8', 'nine': '9',
    'ZERO': '0', 'ONE': '1', 'TWO': '2', 'THREE': '3', 'FOUR': '4',
    'FIVE': '5', 'SIX': '6', 'SEVEN': '7', 'EIGHT': '8', 'NINE': '9',
    'Zero': '0', 'One': '1', 'Two': '2', 'Three': '3', 'Four': '4',
    'Five': '5', 'Six': '6', 'Seven': '7', 'Eight': '8', 'Nine': '9'
}

# Regex pattern for matching digit words
digit_word_pattern = '|'.join(re.escape(word) for word in word_to_digit.keys())
digit_word_full_pattern = rf"\b(?:{digit_word_pattern})\b"

# Regex patterns for matching credit card formats
credit_card_patterns = [
    r'\b(?:\d[ -]*?){13,16}\b',               # Basic format with spaces or hyphens
    r'\b4\d{12,18}\b',                         # Visa (starts with 4, 13 to 19 digits)
    r'\b(?:5[1-5]\d{14}|2[2-7][0-9]{14})\b',  # MasterCard (starts with 51-55 or 2221-2720, 16 digits)
    r'\b3[47]\d{13}\b',                       # American Express (starts with 34 or 37, 15 digits)
    r'\b(?:6011|65|64[4-9])\d{12}\b',         # Discover (starts with 6011 or 65 or 64[4-9], 16 digits)
    r'\b35\d{14}\b',                          # JCB (starts with 35, 16 digits)
    r'\b(?:\d{4}[- ]?){3}\d{4}[- ]?\d{4}[- ]?\d{4}\b'  # Generic credit card pattern
]

def redact_credit_card(text):
    # Compile all credit card patterns into a single regex
    combined_credit_card_pattern = re.compile('|'.join(credit_card_patterns))

    # Replace credit card numbers with '[REDACTED]'
    redacted_text = combined_credit_card_pattern.sub('[REDACTED]', text)
    
    return redacted_text

def replace_digit_words(text):
    # Function to replace matched digit words with corresponding digits
    def convert(match):
        # Convert the matched digit words to digits
        return ''.join(word_to_digit[word] for word in match.group().split())

    # Replace digit words with digits
    return re.sub(digit_word_full_pattern, convert, text)

def process_text(text):
    # Convert digit words to digits first
    text_with_digits = replace_digit_words(text)
    # Then, redact any credit card numbers from the converted text
    final_text = redact_credit_card(text_with_digits)
    return final_text

# Example usage
text = "My credit cards are one two three four five, 1234 5678 9012 3456, and SEVEN EIGHT NINE zero."
processed_text = process_text(text)
print(processed_text)
