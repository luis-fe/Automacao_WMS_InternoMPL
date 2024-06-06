import gc


def lista():
    # Coleta objetos não utilizados
    gc.collect()
    # Lista todos os objetos
    all_objects = gc.get_objects()
    # Imprime o tipo e o número de objetos de cada tipo
    object_types = {}
    for obj in all_objects:
        obj_type = type(obj)
        if obj_type not in object_types:
            object_types[obj_type] = 1
        else:
            object_types[obj_type] += 1
    # Ordena e imprime os tipos de objetos e suas quantidades
    sorted_objects = sorted(object_types.items(), key=lambda item: item[1], reverse=True)
    for obj_type, count in sorted_objects:
        print(f'{obj_type}: {count}')


