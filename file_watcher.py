from watchfiles import watch, Change


previous = {}

def process_trace(file_path):
    global previous
    
    with open(file_path) as f:
        lines = f.readlines()

    header = lines[0].strip().split(",")

    current = {}

    for line in lines[1:]:
        values = line.strip().split(",")
        row = dict(zip(header, values))
        
        task_id = row["task_id"] 
        current[task_id] = row

        if task_id not in previous:
            print("NEW:", row)
        elif previous[task_id] != row:
            print("UPDATED:", row)

    previous = current


for changes in watch('./target.txt'):
    for change, path in changes:
        if change == Change.modified:
            
            process_trace(file_path='./target.txt')
            print(previous)
            

