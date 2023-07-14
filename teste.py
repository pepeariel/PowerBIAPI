
import datetime
current_time = datetime.datetime.now().strftime("%Y-%m-%d %H:%M:%S")
file_path = "output.txt"
with open(file_path, 'w') as file:
    file.write(current_time)