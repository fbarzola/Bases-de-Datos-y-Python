# Clases y herencia
class Auto:
   velocidad = 0
   modelo = ""
   def __init__(self, nam):
     self.modelo = nam
     print(self.modelo,"construido")

   def acelera(self) :
     self.velocidad = self.velocidad + 1
     print(self.modelo,"velocidad",self.velocidad)

# Herencia de clases
# AutoElectrico hereda de Auto
class AutoElectrico(Auto):
   bateria = 0
   def carga(self):
    self.bateria = self.bateria + 3
    print(self.modelo,"bateria",self.bateria)

# Crear instancias de Auto y AutoElectrico
s = Auto("FIAT uno")
s.acelera()

j = AutoElectrico("TESLA s")
j.acelera()
j.carga()

  