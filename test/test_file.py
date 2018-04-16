def bad_append(new_item, a_list=[]):
    print(a_list)
    a_list.append(new_item)
    return a_list

print (bad_append('one'))

print (bad_append('two'))