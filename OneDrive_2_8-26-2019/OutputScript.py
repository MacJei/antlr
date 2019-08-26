class OutputScript():
    def __init__(self, cntx, fname):
        self.cntx = cntx
        self.fname = fname
        self.code = []

    def addCode(self, code):
        self.code.append(code)
    
    def printCode(self):
        with open(self.cntx.target_dir + '\\' + self.fname, 'w') as f:        
            for ln in self.code:
                f.write(ln + "\n")
                
