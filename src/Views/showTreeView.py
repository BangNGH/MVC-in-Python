from tkinter import ttk
import tkinter as tk


class ShowTreeView(tk.Tk):
    PAD = 10
    COLUMN_WIDTH = 200
    THEADER = [
        "Id", "First name", "Last name", "Zipcode", "Price paid" 
    ]
    
    def __init__(self, controller):
        super().__init__()
        self.title("Show Customers")
        self.showTreeViewController = controller
    
        self._make_mainFrame()
        self._make_title()
        self._show_customers()
    
    def main(self):
        self.attributes("-topmost", True)
        self.mainloop()
        
    def _make_mainFrame(self):
        self.frame_main = ttk.Frame(self)
        self.frame_main.pack(padx=self.PAD, pady=self.PAD)
        
    def _make_title(self):
        title = ttk.Label(self.frame_main, text="Customers Manager - Show", font=("Helvetica", 20))
        title.pack(padx=self.PAD, pady=self.PAD)
    
        
    def update(self):
        self.frame_customers.destroy()
        self._show_customers()
    
    '''
    def _createTable(self):
        frame_table = tk.Frame(self.frame_main)
        frame_table.pack()
        self.frame_table = frame_table
        
        # Cria TreeView widget
        tv = ttk.Treeview(frame_table)
        
        # Cria colunas dando nome a elas (para poderem ser referenciadas)
        tv['columns'] = ["fName", "lName", "zip", "pp"]
        
        # Seta informações das colunas
        tv.heading("#0", text="Id", anchor=tk.W)
        tv.column("#0", anchor=tk.W, width=10)

        tv.heading("fName", text="First name")
        tv.column("fName", anchor="center", width=200)
        tv.heading("lName", text="Last name")
        tv.column("lName", anchor="center", width=200)
        tv.heading("zip", text="Zipcode")
        tv.column("zip", anchor="center", width=100)
        tv.heading("pp", text="Price paid")
        tv.column("pp", anchor="center", width=100)
        
        # Coloca tree view no frame
        tv.grid(sticky=(tk.N, tk.S, tk.W, tk.E))
        tv.grid_rowconfigure(0, weight=1)
        tv.grid_columnconfigure(0, weight=1)
        
        # Adiciona listener para habilitar context menu
        tv.bind("<Button-3>", self.contextMenu_display)
        self.tv = tv
    '''
        
    
    def _contextMenu_display(self, event):
        self.contextMenu = tk.Menu(self.frame_main, tearoff=0)
        self.contextMenu.add_command(label="Edit", command=lambda: self.showTreeViewController.btnEdit(self.contextMenu_selectedId))
        self.contextMenu.add_command(label="Delete", command=self.showTreeViewController.btnDel)
        
        # Take data from the row that was clicked
        # Ex: tv.item(data) => {'text': 1, 'image': '', 'values': ['name', 'lastname', 3213, '321.00'], 'open': 0, 'tags': ''}
        rowSelected = self.tv.identify_row(event.y)

        # Check if some data was taken
        if rowSelected:
            # Take data selected and put them in a list
            self.contextMenu_selectedId = self.tv.item(rowSelected)['text']
            
            # Let the row that was clicked as selected
            self.tv.focus(rowSelected)
            self.tv.selection_set(rowSelected)
            
            # Open context menu
            self.contextMenu.selection = self.tv.set(rowSelected)
            self.contextMenu.post(event.x_root, event.y_root)
    
    
    def _show_customers(self):
        customers = self.showTreeViewController.getCustomers()
        
        self.frame_customers = tk.Frame(self.frame_main)
        self.frame_customers.pack(fill="x")
        
        frame_customersView = tk.Frame(self.frame_customers)
        frame_customersView.pack()
        
        # Create TreeView widget
        self.tv = ttk.Treeview(frame_customersView)
        
        # Create columns and name them (so them can be referenced)
        self.tv['columns'] = self.THEADER[1:]
        
        # Put columns info
        # Header
        self.tv.heading("#0", text=self.THEADER[0], anchor=tk.W)
        self.tv.column("#0", anchor=tk.W, width=100)
        for i in range(1, len(self.THEADER)):
            self.tv.heading(self.THEADER[i], text=self.THEADER[i])
            self.tv.column(self.THEADER[i], anchor="center", width=self.COLUMN_WIDTH)
        
        # Data
        for customer in customers:
            self.tv.insert("", tk.END, text=customer[0], values=customer[1:])
        
        # Put tree view on frame
        self.tv.grid(sticky=(tk.N, tk.S, tk.W, tk.E))
        self.tv.grid_rowconfigure(0, weight=1)
        self.tv.grid_columnconfigure(0, weight=1)
        
        # Add listener for enable the context menu
        self.tv.bind("<Button-3>", self._contextMenu_display)
        
        
        btn = ttk.Button(self.frame_customers, text="Update data", command=self.update)
        btn.pack()
