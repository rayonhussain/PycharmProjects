import tkinter as tk
from tkinter import filedialog
import os

root = tk.Tk()
apps=[]

#reading the previous defined apps[] and displaying
if os.path.isfile('save.txt'):
    with open('save.txt','r') as f:
        tempApps=f.read()
        tempApps=tempApps.split(',')
        apps=[x for x in tempApps if x.strip()]

# functions
#function for getting file location
def addApp():
    for widget in frame.winfo_children():
        widget.destroy()

    filename = filedialog.askopenfilename(initialdir="/", title="Select File",
                                          filetypes=(("executables", "*.exe"), ("all files", "*.*")))
    apps.append(filename)
    for app in apps:
        label=tk.Label(frame,text=app,bg="grey")
        label.pack()

#function for running file location
def runApp():
    for app in apps:
        os.startfile(app)


# background color
canvas = tk.Canvas(root, height=600, width=600, bg="#00FFFF")
canvas.pack()

# inner frame
frame = tk.Frame(root, bg="white")
frame.place(relwidth=0.8, relheight=0.8, relx=0.1, rely=0.05)

#buttons
openfile = tk.Button(root, text="Open File", padx=10, pady=5, fg="black", bg="white", command=addApp)
openfile.pack()

runApps = tk.Button(root, text="Run Files", padx=10, pady=5, fg="black", bg="white", command=runApp)
runApps.pack()

#showing 'apps' list
for app in apps:
    label=tk.Label(frame,text=app)
    label.pack()

root.mainloop()

#storing in 'save.txt' of previous directories opened
with open('save.txt','w') as f:
    for app in apps:
        f.write(app +',')