class Vehiculo:
    def __init__(self, marca, modelo):
        self.marca = marca
        self.modelo = modelo
    
    def conducir(self):
        print(f"Conduciendo un {self.marca} {self.modelo}")

class Acuatico:
    def __init__(self, eslora, manga):
        self.eslora = eslora
        self.manga = manga
    
    def navegar(self):
        print(f"Navegando con una eslora de {self.eslora} metros y una manga de {self.manga} metros")

class Barco(Vehiculo, Acuatico):
    def __init__(self, marca, modelo, eslora, manga):
        # Llamamos a los constructores de las clases padre
        Vehiculo.__init__(self, marca, modelo)
        Acuatico.__init__(self, eslora, manga)

# Crear una instancia de Barco
mi_barco = Barco("Empresa", "ModeloX", 30, 8)


# Acceder a métodos de ambas clases
mi_barco.conducir()  # Salida: Conduciendo un Empresa ModeloX
mi_barco.navegar()   # Salida: Navegando con una eslora de 30 
                     #         metros y una manga de 8 metros
