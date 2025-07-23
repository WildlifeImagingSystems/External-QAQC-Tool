def get(file_path):
    with open(file_path, 'r') as file:
        content = file.read()
        return content