class MiClase:
    def __init__(self):
        self.__atributo_privado = 10

    def obtener_atributo_privado(self):
        return self.__atributo_privado

    def modificar_atributo_privado(self, nuevo_valor):
        self.__atributo_privado = nuevo_valor

# Crear una instancia de MiClase
objeto = MiClase()

# Acceder al atributo privado a través de métodos
print(objeto.obtener_atributo_privado())  # Salida: 10

# Modificar el atributo privado a través de métodos
objeto.modificar_atributo_privado(20)
print(objeto.obtener_atributo_privado())  # Salida: 20

# Intentar acceder directamente al atributo privado (no es una práctica recomendada)
# Esto generará un error: AttributeError
print(objeto._MiClase__atributo_privado)
