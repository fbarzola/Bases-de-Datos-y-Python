class Coche:
    def __init__(self, marca, modelo):
        self.marca = marca
        self.modelo = modelo

    def obtener_informacion(self):
        print(f"Coche: {self.marca} {self.modelo}")

# Crear una instancia de Coche
mi_coche = Coche("Toyota", "Corolla")
mi_coche.obtener_informacion()