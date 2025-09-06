class Animal:
    def hacer_sonar(self):
        # Método que será sobrescrito por las subclases
        return "Hola"
class Gato(Animal):
    def hacer_sonar(self):
        return "¡Miau!"

class Perro(Animal):
    def hacer_sonar(self):
        return "¡Guau!"

# Función genérica que hace sonar a un animal
def hacer_sonar_animal(animal):
    return animal.hacer_sonar()

# Crear instancias de las clases Gato y Perro
mi_gato = Gato()
mi_perro = Perro()
otro_animal = Animal()

# Llamar a la función hacer_sonar_animal con diferentes instancias
print(hacer_sonar_animal(mi_gato))  # Salida: ¡Miau!
print(hacer_sonar_animal(mi_perro))  # Salida: ¡Guau!
print(hacer_sonar_animal(otro_animal))  # Salida: Hola
