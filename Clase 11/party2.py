class PartyAnimal:
   x = 0

   def __init__(self):
     print("Estoy construido")

   def party(self) :
     self.x = self.x + 1
     print("Hasta ahora",self.x)

   def __del__(self):
     print("Estoy destruido", self.x)

an = PartyAnimal()
an.party()
an.party()
an = 42
print("an contiene",an)