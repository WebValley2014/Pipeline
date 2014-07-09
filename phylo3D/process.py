#!/usr/bin/env python

from graphlan_lib import CircTree as CTree
import math
import sys 

if __name__ == "__main__":
    # load tree
    ctree = CTree( sys.argv[1], 1 )
    ctree.positions = 0

    # draw tree - build model - can do it later?
    ctree.draw( 'temp.pdf', out_format = 'pdf', out_dpi = 72, out_size = 7.0, out_pad = 0.5 )


    # load supplementary data - WHAT A SMART IDEA WOULD HAVE BEEN TO TEACH/USE PANDAS!
    # FILE FORMAT
    # 0: FEATURE_ID
    # 1: FEATURE_NAME
    # 2: MEAN_POS
    # 3: MEDIAN_ALL
    # 4: MEDIAN_0
    # 5: MEDIAN_1
    # 6: FOLD_CHANGE
    # 7: LOG2_FOLD_CHANGE

    filename = sys.argv[2]
    associations = {}
    alias = {}
    h = open(filename, 'r')
    header = True
    for i in h:
        i = i.strip()
        i = i.split("\t")

        # recover real name
        name = i[1].split(";")
        alias["_" + i[0]] = name[-1]


        j = "_" + i[0] # ID
        k = i[2] # mean position
        kd = 0
        if not header:
            k2 = float(i[4]) # median class 1
            k3 = float(i[5]) # median class 2
            kd = k2 - k3
        print kd
        associations[j] = (k,kd)
        header = False
    h.close()






    # prune the tree
    albero = ctree.tree
    stringa = "{\n\"data\":[ "
    dati = []
    nomi = {}
    whole = []
    whole.append(albero.clade)
    cid = 0
    albero.clade.id = cid
    albero.clade.depth = 0
    cid += 1
    while len(whole) > 0:
        current = whole.pop()
        cdepth = current.depth
        tempo = []
        refactored = []
        for i in current.clades:
            refactored.append(i)
            i.id = cid
            i.depth = cdepth + 1
            tempo.append(i.id)
            cid += 1
            whole.append(i)
        current.clades = refactored
        dati.append((current.id, current.name, current.r, current.theta, tempo, cdepth))


    for i in range(0, len(dati)):
        angle = dati[i][3]
        r = dati[i][2]
        idc = dati[i][0]
        name = dati[i][1]
        rank = 0
        cha = 0
        if name in associations:
            rank = associations[name][0]
            cha = associations[name][1]
            # print rank
        children = dati[i][4]
        depth = dati[i][5]
        x=r*math.cos(angle)
        y=r*math.sin(angle)
        # print r, angle, x, y
        cstringa = "\n" + "{" 
        cstringa = cstringa + "\"id\": " + str(idc)
        if name in alias:
            cstringa = cstringa + ", \"name\": \"" + str(alias[name]) + "\""
        else:
            cstringa = cstringa + ", \"name\": \"" + str(name) + "\""
        cstringa = cstringa + ", \"x\": " + str(x)
        cstringa = cstringa + ", \"y\": " + str(y)
        cstringa = cstringa + ", \"rank\": " + str(rank)
        cstringa = cstringa + ", \"childs\": " + str(children)
        cstringa = cstringa + ", \"depth\": " + str(depth)
        cstringa = cstringa + ", \"change\": " + str(cha)
        cstringa = cstringa + "},"
        stringa = stringa + cstringa

    stringa  = stringa[0:-1] + "\n]}\n"

    text_file = open('temp.json', "w")
    text_file.write(stringa)
    text_file.close()






    # prune the tree
    albero = ctree.tree
    stringa = "{\n\"data\":[ "
    dati = []
    nomi = {}
    whole = []
    whole.append(albero.clade)
    cid = 0
    albero.clade.id = cid
    albero.clade.depth = 0
    cid += 1
    while len(whole) > 0:
        current = whole.pop()
        cdepth = current.depth
        tempo = []
        if len(current.clades) == 1:
            if current.name in associations:
                rank = associations[current.name][0]
            else:
                rank = 0
            if current.clades[0].name in associations:
                rankc = associations[current.clades[0].name][0]
            else:
                rankc = 0
            if rankc <= rank:
                # print "QUI"
                current.clades = current.clades[0].clades
                whole.append(current)
                continue

        refactored = []
        for i in current.clades:
            if len(i.clades) == 0:
                if current.name in associations:
                    rank = associations[current.name][0]
                else:
                    rank = 0
                if i.name in associations:
                    rankc = associations[i.name][0]
                else:
                    rankc = 0
                if rankc <= rank:
                    # print "QUI2"
                    # current.clades = current.clades[0].clades
                    continue
            refactored.append(i)
            i.id = cid
            i.depth = cdepth + 1
            tempo.append(i.id)
            cid += 1
            whole.append(i)
        current.clades = refactored

        # for i in current.clades:
        #     i.id = cid
        #     i.depth = cdepth + 1
        #     tempo.append(i.id)
        #     cid += 1
        #     whole.append(i)
        dati.append((current.id, current.name, current.r, current.theta, tempo, cdepth))
    # print dati
    # print associations

    # draw tree - build model - can do it later?
    ctree.draw( 'temp1.pdf', out_format = 'pdf', out_dpi = 72, out_size = 7.0, out_pad = 0.5 )

    # ctree.draw( 'prova2.pdf', 
    #             out_format = args['format'], 
    #             out_dpi = args['dpi'],
    #             out_size = args['size'],
    #             out_pad = args['pad'] )


    
    # prune again
    albero = ctree.tree
    stringa = "{\n\"data\":[ "
    dati = []
    nomi = {}

    whole = []
    whole.append(albero.clade)
    cid = 0
    albero.clade.id = cid
    albero.clade.depth = 0
    cid += 1
    while len(whole) > 0:
        current = whole.pop()
        cdepth = current.depth
        tempo = []
        if len(current.clades) == 1:
            if current.name in associations:
                rank = associations[current.name][0]
            else:
                rank = 0
            if current.clades[0].name in associations:
                rankc = associations[current.clades[0].name][0]
            else:
                rankc = 0
            if rankc <= rank:
                # print "QUI"
                current.clades = current.clades[0].clades
                whole.append(current)
                continue

        refactored = []
        for i in current.clades:
            if len(i.clades) == 0:
                if current.name in associations:
                    rank = associations[current.name][0]
                else:
                    rank = 0
                if i.name in associations:
                    rankc = associations[i.name][0]
                else:
                    rankc = 0
                if rankc <= rank:
                    # print "QUI2"
                    # current.clades = current.clades[0].clades
                    continue
            refactored.append(i)
            i.id = cid
            i.depth = cdepth + 1
            tempo.append(i.id)
            cid += 1
            whole.append(i)
        current.clades = refactored
        dati.append((current.id, current.name, current.r, current.theta, tempo, cdepth))
    # print dati
    # print associations
    # print "FINE"





    for i in range(0, len(dati)):
        angle = dati[i][3]
        r = dati[i][2]
        idc = dati[i][0]
        name = dati[i][1]
        rank = 0
        cha = 0
        if name in associations:
            rank = associations[name][0]
            cha = associations[name][1]
            # print rank
        children = dati[i][4]
        depth = dati[i][5]
        x=r*math.cos(angle)
        y=r*math.sin(angle)
        # print r, angle, x, y
        cstringa = "\n" + "{" 
        cstringa = cstringa + "\"id\": " + str(idc)
        if name in alias:
            cstringa = cstringa + ", \"name\": \"" + str(alias[name]) + "\""
        else:
            cstringa = cstringa + ", \"name\": \"" + str(name) + "\""
        cstringa = cstringa + ", \"x\": " + str(x)
        cstringa = cstringa + ", \"y\": " + str(y)
        cstringa = cstringa + ", \"rank\": " + str(rank)
        cstringa = cstringa + ", \"childs\": " + str(children)
        cstringa = cstringa + ", \"depth\": " + str(depth)
        cstringa = cstringa + ", \"change\": " + str(cha)
        cstringa = cstringa + "},"
        stringa = stringa + cstringa

    stringa  = stringa[0:-1] + "\n]}\n"

    text_file = open(sys.argv[3], "w")
    text_file.write(stringa)
    text_file.close()
#
