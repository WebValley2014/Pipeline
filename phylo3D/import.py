# import matplotlib
# # matplotlib.use('Qt4Agg')
# matplotlib.use('tkagg')

import ete2
from ete2 import Tree, TreeNode
import sys
from Bio import Phylo

# def build_vis():
# 	ts = TreeStyle()
# 	ts.mode = "c"
# 	ts.arc_start = 0 # 0 degrees = 3 o'clock
# 	ts.arc_span = 360
# 	ts.layout_fn = my_layout # Use custom layout
# 	return ts
# #

# def my_layout(node):
# 	style2 = NodeStyle()
# 	style2["fgcolor"] = "#000000"
# 	style2["shape"] = "circle"
# 	node.img_style = style2
#  	node.img_style["bgcolor"] = "LightSteelBlue"
# 	if node.is_leaf():
# 		node.img_style["size"] = 40
# 		node.img_style["fgcolor"] = "#FFFFFF"
# 		pass
# 	else:
# 		node.img_style["size"] = 40
# 		node.img_style["shape"] = "sphere"
# 		node.img_style["fgcolor"] = "#FFFFFF"

# 	tempt = AttrFace('name', fsize=80)
# 	tempt.fgcolor = "Black"
# 	tempt.margin_top = 10
# 	tempt.margin_right = 10
# 	tempt.margin_left = 10
# 	tempt.margin_bottom = 10
# 	tempt.opacity = 0.5 # from 0 to 1
# 	tempt.inner_border.width = 1 # 1 pixel border
# 	tempt.inner_border.type = 1  # dashed line
# 	tempt.border.width = 1
# 	node.add_face(tempt, column=0, position="branch-top")
# #

def search_by_name(node, name):
	matches = []
	for i in node.children:
		if i.name == name:
			# print i
			return i			
	return None
#

if __name__ == '__main__':
	t = Tree()
	# filename = './PRE_ML/names.txt'
	filename = 'test/2fc72698-024a-4776-a6fd-73e9914e69ff.featurelist.txt'
	filename = sys.argv[1]
	associations = {}
	header = True
	h = open(filename, 'r')
	for i in h:
		if header:
			header = False
			continue
		i = i.strip()
		i = i.split("\t")
		j = i[1].split(";")
		current = t
		# associations[i[0]] = 
		for k in range(0,len(j)):
			# print k
			pos = search_by_name(current, j[k])
			if pos == None:
				print "Adding " + j[k]
				# print pos
				pos = current.add_child(TreeNode())
				pos.dist=0.2
				pos.name = j[k]
				pos.alias = i[0]
			current = pos
	h.close()

	for node in t.traverse("postorder"):
		if node.name == "NoName":
			pass
		else:
	  		node.name = "_" + str(node.alias)

	t.write(features=["name"], outfile="temp.nw", format=1)
	Phylo.convert('temp.nw', 'newick', sys.argv[2], 'phyloxml')
#
