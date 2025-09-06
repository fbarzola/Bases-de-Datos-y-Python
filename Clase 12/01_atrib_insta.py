class Ejemplo:
    def __init__(self, atributo):
        self.atributo = atributo # Atributo de instancia
        
# Crear dos instancias de la clase Ejemplo
instancia1 = Ejemplo(10)
instancia2 = Ejemplo(20)

# Acceso al atributo de instancia
print("Atributo de instancia instancia1:", instancia1.atributo)  # Salida: 10
print("Atributo de instancia instancia2:", instancia2.atributo)  # Salida: 20

# Modificar el atributo de instancia de instancia1
instancia1.atributo = 30
print("Atributo de instancia instancia1:", instancia1.atributo)  # Salida: 30
print("Atributo de instancia instancia2:", instancia2.atributo)  # Salida: 20
