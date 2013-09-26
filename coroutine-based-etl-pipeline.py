#!/usr/local/bin/python
# encoding: utf-8


# FIXME replace \n in individual fields w/ a space
# TODO understand why quotechar
# TODO get rid of backshashes '\' in sizes
# TODO parse rug sizes from rug name

'''
a coroutine based data processing pipeline; 
this pipeline is comprised of three tiers:
	(i) input (source) tier: only send (and yield as entry point)
	(ii) middle tier (workers): both yield and send
	(iii) output (sink) tier: only yield 
	
	a coroutine-based pipeline has one fn for the first tier and 
	one for the third, and from one to many for the middle tier.
'''

import os, time, re
import csv as CSV
import json as JSON
import itertools
import fnmatch
from functools import wraps
import time
import cProfile

err = [],
data_file = [['Graceful Armchair', 'Furniture-Other-Other', 'Barbara Barry Tag Sale(BAB)', "This Week's Vintage Mix", 'walnut/linen/rayon', '', 'dark walnut/sky blue', '', '699.00'],
['Regency Dining Table', 'Furniture-Tables-Dining Tables', 'Baker(BAK)', 'Fine Design', 'alder/cherry wood', '', 'chestnut', '', '3799.00'],
['Entertainment Unit', 'Furniture-Armoires, Cabinets, & Dressers-Media & TV', 'Baker(BAK)', 'Fine Design', 'English sycamore', '', 'presidio finish', '', '11999.00'],
['Rubann Mirror', 'Wall Decor-Mirrors-Mirrors', 'Baker(BAK)', 'Fine Design', 'hardwood/glass', '', 'espresso', '', '1399.00'],
['Upholstered Side Chair', 'Furniture-Chairs & Stools-Dining Chairs', 'Baker(BAK)', 'Fine Design', 'frame, hardwood solids; back, cane; upholstery, leather', '', 'frame, aged European brown; upholstery, dark espresso', '', '599.00'],
['Silk Bolster Pillow, Slate', 'Textiles-Decorative Pillows-Indoor', 'Baker(BAK)', 'Fine Design', 'silk', '', 'slate/ivory', '', '139.00'],
['Striped 12x18 Pillow, Multi', 'Textiles-Decorative Pillows-Indoor', 'Baker(BAK)', 'Fine Design', 'wool/sateen', '', 'sky/moss/ivory', '', '99.00'],
['Milling Road Rattan Ottoman', 'Furniture-Ottomans-Ottomans', 'Baker(BAK)', 'Fine Design', 'rattan', '', 'pecan', '', '279.00'],
['Ming Stacking Tables', 'Furniture-Other-Other', 'Baker(BAK)', 'Fine Design', 'hardwood', '', 'espresso', '', '349.00'],
['Silk Bolster Pillow, Moss', 'Textiles-Decorative Pillows-Indoor', 'Baker(BAK)', 'Fine Design', 'silk', '', 'moss/ivory', '', '139.00'],
['Wool Throw, Plaid', 'Textiles-Decorative Throws-Decorative Throws', 'Baker(BAK)', 'Fine Design', 'wool', '', 'caledon', '', '339.00'],
['Signature 17x17 Pillow, Oyster', 'Textiles-Decorative Pillows-Indoor', 'Baker(BAK)', 'Fine Design', 'wool/sateen', '', 'oyster/khaki', '', '199.00'],
['BIA Cordon Bleu Square Baker, Red', 'Kitchen & Tabletop-Bakeware-Bakers, Casserole Dishes & Ramekins', 'BIA Cordon Bleu(BIA)', 'A Welcoming Table', 'stoneware', '', 'red', '', '19.00'],
['Core Bamboo Cutting Boards, S/2', 'Kitchen & Tabletop-Tools-Cutting Boards', '', 'Homegrown', 'bamboo', '', 'natural', '', '12.00'],
['Heirloom Secretary Desk', 'Furniture-Desks-Desks', 'David Francis Furniture(DAV)', 'The Essential Guide', 'frame, mahogany/leather; hardware, brass', '', 'frame, golden mahogany/sepia; hardware, pewter', '', '3199.00'],
['Wavy Sterling Frame, 4x6', 'Decorative Accessories-Accents-Picture Frames', 'Eccolo(ECC)', 'Your Dressing Room', '.925 sterling silver', '', 'silver', '', '79.00'],
['Chased Border Sterling Frame, 4x6', 'Decorative Accessories-Accents-Picture Frames', 'Eccolo(ECC)', 'Your Dressing Room', '.925 sterling silver', '', 'silver', '', '149.00'],
['Elastico Journal, Black', 'Lifestyle & Leisure-Office-Desk Accessories', 'Eccolo(ECC)', "The Gentleman's Desk", 'leather', '', 'black', '', '24.00'],
['Swirl Sterling Frame, 4x6', 'Decorative Accessories-Accents-Picture Frames', 'Eccolo(ECC)', 'The Refined Vanity', '.925 sterling silver', '', 'silver', '', '79.00'],
['Diamond-Corners Sterling Frame, 4x6', 'Decorative Accessories-Accents-Picture Frames', 'Eccolo(ECC)', 'The Refined Vanity', '.925 sterling silver', '', 'silver', '', '79.00'],
['Thin Beaded Sterling Frame, 4x6', 'Decorative Accessories-Accents-Picture Frames', 'Eccolo(ECC)', 'Your Dressing Room', '.925 sterling silver', '', 'silver', '', '79.00'],
['Narrow Hammered Sterling Frame, 4x6', 'Decorative Accessories-Accents-Picture Frames', 'Eccolo(ECC)', 'Your Dressing Room', '.925 sterling silver', '', 'silver', '', '79.00'],
['Flowered Corners Sterling Frame, 4x6', 'Decorative Accessories-Accents-Picture Frames', 'Eccolo(ECC)', 'Your Dressing Room', '.925 sterling silver', '', 'silver', '', '79.00'],
['Round Edge w/ Bead Sterling Frame, 4x6', 'Decorative Accessories-Accents-Picture Frames', 'Eccolo(ECC)', 'The Refined Vanity', '.925 sterling silver', '', 'silver', '', '79.00'],
['Smooth Beaded Sterling Frame, 4x6', 'Decorative Accessories-Accents-Picture Frames', 'Eccolo(ECC)', 'Your Dressing Room', '.925 sterling silver', '', 'silver', '', '135.00'],
['Lanes Bath Sheet, White', 'Bed & Bath-Bath-Towels', 'Frette(FRT)', 'Luxurious Bed & Bath Linens', 'cotton; 650 GSM', '', 'white', '', '39.00'],
['Mint Julep Cup, Large', 'Kitchen & Tabletop-Barware-Other', 'Godinger(GOD)', 'All the Small Things', 'silver-plated brass', '', 'silver', '', '28.00'],
['Godinger Bottle-Opener & Corkscrew', 'Kitchen & Tabletop-Barware-Bar Tools & Accessories', 'Godinger(GOD)', 'Happy Hour', 'silver-plate/mother-of-pearl', '', 'silver', '', '22.00'],
['Short-Arm Signature Large Chair, Stripe', 'Furniture-Chairs & Stools-Club Chairs & Wingbacks', '', 'Coveted Classics', 'frame, beech; upholstery, linen; fill, feather/down; casters, brass', '', 'legs, mahogany finish; upholstery, red', '', '4299.00'],
['Simply Dahl Chair, Chocolate', 'Furniture-Chairs & Stools-Club Chairs & Wingbacks', '', 'Coveted Classics', 'frame, beech; upholstery, velvet/cotton; fill, down/feather; casters, brass', '', 'legs, mahogany finish; upholstery, dark chocolate/rose', '', '4299.00'],
['Short-Arm Signature Large Chair, Floral', 'Furniture-Chairs & Stools-Club Chairs & Wingbacks', '', 'Coveted Classics', 'frame, beech; upholstery, linen; fill, feather/down; casters, brass', '', 'legs, mahogany finish; grayswood/multi', '', '4799.00'],
['Square Stool, Emerald', 'Furniture-Benches-Benches', '', 'Coveted Classics', 'frame, beech; upholstery, mohair; fill, feather/down', '', 'legs, spruce; upholstery, emerald', '', '1499.00'],
['Chandelier Print Shower Curtain', 'Bed & Bath-Bath-Shower Curtains & Mats', 'Izola(IZO)', 'Downstairs', 'polyester/nickel-plated grommets', '', 'black/white', '', '19.00'],
['Venice, Italy Shower Curtain', 'Bed & Bath-Bath-Shower Curtains & Mats', 'Izola(IZO)', 'Downstairs', 'polyvinyl chloride (PVC)/nickel-plated grommets', '', 'black/white', '', '10.00'],
['Pine Tree 14x20 Pillow, Iris', 'Textiles-Decorative Pillows-Indoor', 'Kevin OBrien(KOB)', 'Alluring Accents', 'cover, 60% silk/40% rayon velvet; fill, 95% feather/5% down', '', 'iris', '', '99.00'],
['Ditsy 8x16 Velvet Pillow, Teal', 'Textiles-Decorative Pillows-Indoor', 'Kevin OBrien(KOB)', 'Alluring Accents', 'cover, 50% silk/50% rayon velvet; insert, polyester', '', 'teal', '', '49.00'],
['Pine 16x16 Pillow, Mint', 'Textiles-Decorative Pillows-Indoor', 'Kevin OBrien(KOB)', 'Alluring Accents', 'front, silk/rayon velvet; back, solid dupioni; fill, 95% feather/5% down', '', 'mint/blush', '', '99.00'],
['Kelly Garden Stool, Silver', 'Furniture-Chairs & Stools-Garden Stools', 'Legend of Asia(LOA)', 'Chinoiserie Chic', 'high-fire porcelain', '', 'silver', '', '109.00'],
['Three-Fish Garden Stool, Blue/White', 'Furniture-Chairs & Stools-Garden Stools', 'Legend of Asia(LOA)', 'Chinoiserie Chic', 'high-fire porcelain', '', 'blue/white', '', '119.00'],
['Dragon Medallion Stool, Turquoise', 'Furniture-Chairs & Stools-Garden Stools', 'Legend of Asia(LOA)', 'Chinoiserie Chic', 'high-fire porcelain', '', 'distressed turquoise', '', '129.00'],
['Garden Stool, Grass Green', 'Furniture-Chairs & Stools-Garden Stools', 'Legend of Asia(LOA)', 'Chinoiserie Chic', 'high-fire porcelain', '', 'green', '', '129.00'],
['Kelly Garden Stool, Egg Shell', 'Furniture-Chairs & Stools-Garden Stools', 'Legend of Asia(LOA)', 'Chinoiserie Chic', 'high-fire porcelain', '', 'eggshell', '', '129.00'],
['Kelly Garden Stool, Gold', 'Furniture-Chairs & Stools-Garden Stools', 'Legend of Asia(LOA)', 'Chinoiserie Chic', 'high-fire porcelain', '', 'gold', '', '109.00'],
['Kelly Garden Stool, Steel Blue', 'Furniture-Chairs & Stools-Garden Stools', 'Legend of Asia(LOA)', 'Chinoiserie Chic', 'high-fire porcelain', '', 'steel blue', '', '129.00'],
['Kelly Garden Stool, Green Crackle', 'Furniture-Chairs & Stools-Garden Stools', 'Legend of Asia(LOA)', 'Chinoiserie Chic', 'high-fire porcelain', '', 'green', '', '109.00'],
['Sitting Foo Dogs, Asst. of 2', 'Decorative Accessories-Objets-Objects', 'Legend of Asia(LOA)', 'Chinoiserie Chic', 'porcelain', '', 'lime green', '', '109.00'],
['Kelly Garden Stool, White', 'Furniture-Chairs & Stools-Garden Stools', 'Legend of Asia(LOA)', 'Chinoiserie Chic', 'high-fire porcelain', '', 'white', '', '99.00'],
['Eight-Immortals Garden Stool, Navy', 'Furniture-Chairs & Stools-Garden Stools', 'Legend of Asia(LOA)', 'Chinoiserie Chic', 'high-fire porcelain', '', 'navy/white', '', '129.00'],
['Kelly Garden Stool, Orange/White', 'Furniture-Chairs & Stools-Garden Stools', 'Legend of Asia(LOA)', 'Chinoiserie Chic', 'high-fire porcelain', '', 'orange/white', '', '129.00'],
['Kelly Garden Stool, Sapphire Blue', 'Furniture-Chairs & Stools-Garden Stools', 'Legend of Asia(LOA)', 'Chinoiserie Chic', 'high-fire porcelain', '', 'sapphire blue', '', '119.00'],
['Kelly Garden Stool, Celadon', 'Furniture-Chairs & Stools-Garden Stools', 'Legend of Asia(LOA)', 'Chinoiserie Chic', 'high-fire porcelain', '', 'celadon', '', '129.00'],
['Bamboo Garden Stool, Celadon', 'Furniture-Chairs & Stools-Garden Stools', 'Legend of Asia(LOA)', 'Chinoiserie Chic', 'high-fire porcelain', '', 'celadon', '', '139.00'],
['Kelly Garden Stool, Cobalt Blue', 'Furniture-Chairs & Stools-Garden Stools', 'Legend of Asia(LOA)', 'Chinoiserie Chic', 'high-fire porcelain', '', 'cobalt blue', '', '109.00'],
['Lion-Motif Garden Stool, Blue/White', 'Furniture-Chairs & Stools-Garden Stools', 'Legend of Asia(LOA)', 'Chinoiserie Chic', 'high-fire porcelain', '', 'blue/white', '', '119.00'],
['Dragon Lotus Gourd Vase', 'Decorative Accessories-Accents-Vases', 'Legend of Asia(LOA)', 'Chinoiserie Chic', 'porcelain', '', 'celadon/white', '', '119.00'],
['16" Silver Athena Tray', 'Decorative Accessories-Accents-Trays', 'Lunares(LUA)', 'Tray Chic', 'aluminum alloy', '', 'silver', '', '59.00'],
['Large Water Bowl, Cane', 'Baby, Kids & Pets-Pets-Accessories', 'Mason Cash(MAC)', 'Downstairs', 'earthenware', '', 'cane', '', '19.00'],
['Melissa & Doug Pound-a-Peg', 'Baby, Kids & Pets-Kids-Toys', 'Melissa & Doug(MAD)', 'Top Toys', 'wood', '', 'multi', '', '8.00'],
['Magnetic Hide & Seek Board', 'Baby, Kids & Pets-Kids-Toys', 'Melissa & Doug(MAD)', 'Top Toys', 'wood', '', 'multi', '', '14.00'],
['Pull-Along Zoo Animals', 'Baby, Kids & Pets-Kids-Toys', 'Melissa & Doug(MAD)', 'Top Toys', 'wood', '', 'multi', '', '16.00'],
['Melissa & Doug Alphabet Sound Puzzle', 'Baby, Kids & Pets-Kids-Toys', 'Melissa & Doug(MAD)', 'Top Toys', 'wood', '', 'multi', '', '16.00'],
['Melissa & Doug Shape Sorting Clock', 'Baby, Kids & Pets-Kids-Toys', 'Melissa & Doug(MAD)', 'Top Toys', 'wood', '', 'yellow/multi', '', '12.00'],
['Large Dog Bowl, Cane', 'Baby, Kids & Pets-Pets-Accessories', 'Mason Cash(MAC)', 'Downstairs', 'earthenware', '', 'cane', '', '16.00'],
['Horse Carrier', 'Baby, Kids & Pets-Kids-Toys', 'Melissa & Doug(MAD)', 'Top Toys', 'wood/plastic/MDF/paint/ flocking/metal', '', 'red/natural', '', '16.00'],
['Melissa & Doug 100 Wood Blocks Set', 'Baby, Kids & Pets-Kids-Toys', 'Melissa & Doug(MAD)', 'Top Toys', 'wood/non-toxic paint', '', 'multi', '', '16.00'],
['Melissa & Doug Bake/Decorate Cupcake Set', 'Baby, Kids & Pets-Kids-Toys', 'Melissa & Doug(MAD)', 'Top Toys', 'plastic', '', 'multi', '', '16.00'],
['Easel Accessory Set', 'Baby, Kids & Pets-Kids-Toys', 'Melissa & Doug(MAD)', 'Top Toys', 'paint/plastic', '', 'multi', '', '28.00'],
['S/3 Stacking Construction Vehicles', 'Baby, Kids & Pets-Kids-Toys', 'Melissa & Doug(MAD)', 'Top Toys', 'wood', '', 'multi', '', '16.00'],
['Melissa & Doug Wooden Standing Art Easel', 'Baby, Kids & Pets-Kids-Toys', 'Melissa & Doug(MAD)', 'Top Toys', 'wood/plastic', '', 'multi', '', '64.00'],
['Hammered Copper Ice Bucket', 'Kitchen & Tabletop-Barware-Bar Tools & Accessories', 'Mauviel(MVL)', 'Add to the Essentials', 'copper', '', 'copper/gold', '', '219.00'],
['saute pan 24 cm with lid', 'Kitchen & Tabletop-Cookware-Cooking Pans', 'Mauviel(MVL)', 'The Top 50', 'copper/stainless steel', '', 'copper/silver', '', '385.00'],
['saucepan 16 cm with lid', 'Kitchen & Tabletop-Cookware-Cooking Pans', 'Mauviel(MVL)', 'The Top 50', 'copper/stainless steel', '', 'copper/silver', '', '249.00'],
['Hammered Copper Wine Bucket', 'Kitchen & Tabletop-Barware-Bar Tools & Accessories', 'Mauviel(MVL)', 'Add to the Essentials', 'copper', '', 'copper/gold', '', '269.00'],
['2-Bottle Carrier, Black', 'Lifestyle & Leisure-Outdoor Games & Recreation-Picnic & Tailgating Accessories', 'Picnic at Ascot(PCA)', 'Open-Air Afternoons', '600D polycanvas; frame, aluminum', '', 'black', '', '22.00'],
['Vienna Coffee Tote for 2, Black', 'Lifestyle & Leisure-Outdoor Games & Recreation-Picnic & Tailgating Accessories', 'Picnic at Ascot(PCA)', 'Open-Air Afternoons', '600-denier polycanvas', '', 'black', '', '39.00'],
['Portable Picnic Table Set, Brown', 'Lifestyle & Leisure-Outdoor Games & Recreation-Picnic & Tailgating Accessories', 'Picnic at Ascot(PCA)', 'Open-Air Afternoons', 'wood/aluminum', '', 'brown/silver', '', '99.00'],
['Wheeled Picnic Cooler for 4, Black', 'Lifestyle & Leisure-Outdoor Games & Recreation-Picnic & Tailgating Accessories', 'Picnic at Ascot(PCA)', 'Open-Air Afternoons', '600-denier polycanvas', '', 'black', '', '79.00'],
['Insulated Tote Bag, Navy', 'Lifestyle & Leisure-Outdoor Games & Recreation-Picnic & Tailgating Accessories', 'Picnic at Ascot(PCA)', 'Open-Air Afternoons', 'natural woven fiber/600-denier polycanvas', '', 'navy', '', '22.00'],
['Portable Picnic Seat & Blanket, Brown', 'Lifestyle & Leisure-Outdoor Games & Recreation-Picnic & Tailgating Accessories', 'Picnic at Ascot(PCA)', 'Open-Air Afternoons', 'seat, 600D polycanvas; blanket, acrylic', '', 'brown', '', '39.00'],
['Collapsible Basket Cooler, Black', 'Housewares-Storage & Baskets-Other', 'Picnic at Ascot(PCA)', 'Open-Air Afternoons', 'cooler, 600-denier polycanvas; frame/handles, aluminum', '', 'black', '', '35.00'],
['Navy Stadium Seat  w/ Tan Blanket, Navy', 'Lifestyle & Leisure-Outdoor Games & Recreation-Picnic & Tailgating Accessories', 'Picnic at Ascot(PCA)', 'Open-Air Afternoons', 'seat, 600-denier polycanvas; blanket, acrylic', '', 'seat, navy; blanket, tan', '', '39.00'],
['Collapsible Basket Cooler, Navy', 'Housewares-Storage & Baskets-Other', 'Picnic at Ascot(PCA)', 'Open-Air Afternoons', '600-denier polycanvas;<br/> frame/handles, aluminum', '', 'navy', '', '35.00'],
['Willow Picnic Basket for 4, Stripes', 'Lifestyle & Leisure-Outdoor Games & Recreation-Picnic & Tailgating Accessories', 'Picnic at Ascot(PCA)', 'Open-Air Afternoons', 'basket, natural willow/leather', '', 'natural; napkins/plates, Santa Cruz striped', '', '179.00'],
['Cutting Board & Tools Set, Natural', 'Kitchen & Tabletop-Serveware-Cheese Boards & Knives', 'Picnic at Ascot(PCA)', 'Open-Air Afternoons', 'board, bamboo; dish, ceramic; knives, stainless steel', '', 'board, natural; dish, white; knives, silver', '', '32.00'],
['Trunk Organizer and Cooler, Black', 'Lifestyle & Leisure-Outdoor Games & Recreation-Picnic & Tailgating Accessories', 'Picnic at Ascot(PCA)', 'Eat, Drink, Cheer', '600-denier polycanvas', '', 'black', '', '42.00'],
['Collapsible Basket Cooler, Orange', 'Lifestyle & Leisure-Outdoor Games & Recreation-Picnic & Tailgating Accessories', 'Picnic at Ascot(PCA)', 'Open-Air Afternoons', '600-denier polycanvas;<br/> frame/handles, aluminum', '', 'orange', '', '35.00'],
['Nautilus Shell w/ Silver Feet, White', 'Decorative Accessories-Objets-Objects', 'Philmore USA(PHI)', 'Something to Talk About', 'white nautilus pompilius/sterling silver', '', 'white/silver', '', '149.00'],
['Jungle Starfish Votive Holder', 'Decorative Accessories-Accents-Candleholders', 'Philmore USA(PHI)', 'Downstairs', 'starfish/sterling silver', '', 'rust brown/silver', '', '75.00'],
['5-Pc Natura Flatware Set, Pearl', 'Kitchen & Tabletop-Flatware-Flatware Place Settings', 'Sabre(SAB)', 'All Things French', '18/10 stainless steel; handles, resin', '', 'pearl/silver', '', '72.00'],
['5-Pc Natura Flatware Set  Tortoise', 'Kitchen & Tabletop-Flatware-Flatware Place Settings', 'Sabre(SAB)', 'Rustic & Natural', '18/10 stainless steel/resin', '', 'tortoise/silver', '', '62.00'],
['3-Pc Natura Cheese Set, Pearl', 'Kitchen & Tabletop-Serveware-Cheese Boards & Knives', 'Sabre(SAB)', 'All Things French', '18/10 stainless steel; handles, resin', '', 'pearl/silver', '', '49.00'],
['2-Pc Natura Serving Set  Tortoise', 'Kitchen & Tabletop-Serveware-Serving Pieces & Hostess Sets', 'Sabre(SAB)', 'Rustic & Natural', 'resin handles; 18/10 stainless steel', '', 'tortoise/silver', '', '65.00'],
['Fouta Multi Stripe Towel, Light Green', 'Bed & Bath-Bath-Towels', 'Scents and Feel(SAF)', 'Fouta Towels, Scarves & More', 'cotton', '', 'green/multi', '', '30.00'],
['Pagoda Candle, Cutting Garden', 'Lifestyle & Leisure-Candles-Candles', 'Seda(SED)', 'A Treat for the Senses', 'candle, soy/paraffin wax blend; vessel, glass', '', 'candle, white; packaging, green/yellow', '', '35.00'],
["Pagoda Candle, L'Orangerie", 'Lifestyle & Leisure-Candles-Candles', 'Seda(SED)', 'A Treat for the Senses', 'candle, soy/paraffin wax blend; vessel, glass', '', 'candle, white; packaging, orange/aqua', '', '35.00'],
['Pagoda Candle, Alabaster Lily', 'Lifestyle & Leisure-Candles-Candles', 'Seda(SED)', 'A Treat for the Senses', 'candle, soy/paraffin wax blend; vessel, glass', '', 'candle, white; packaging, yellow/aqua', '', '35.00'],
['Woven Square Tissue Cover, Mahogany', 'Bed & Bath-Bath-Bath Accessories', '', 'One-Stop Shop', 'mahogany', '', 'mahogany', '', '39.00'],
['Pagoda Candle,Napoleon Lemon', 'Lifestyle & Leisure-Candles-Candles', 'Seda(SED)', 'A Treat for the Senses', 'candle, soy/paraffin wax blend; vessel, glass', '', 'candle, white; packaging, blue/yellow', '', '35.00'],
['Woven Cotton Ball Box, Mahogany', 'Bed & Bath-Bath-Bath Accessories', '', 'One-Stop Shop', 'woven mahogany', '', 'mahogany', '', '29.00'],
['Pagoda Candle, Camellia Montagne', 'Lifestyle & Leisure-Candles-Candles', 'Seda(SED)', 'A Treat for the Senses', 'candle, soy/paraffin wax blend; vessel, glass', '', 'candle, white; packaging, orange/aqua', '', '35.00']]


D = []

#-------------- filters -----------------#

hiprice = lambda q: float(q[-1]) >= 5000
isrug = lambda q: 'rug' in q[0].replace(',', '').lower().split()

#------------------------- wrapper functions ------------------------#

def timerfn(fn):
	@wraps(fn)
	def wrapper(*args, **kwargs):
		start = time.perf_counter()
		q = fn(*args, **kwargs)
		end = time.perf_counter()
		print("{}.{}: {} sec".format(fn.__module__, fn.__name__, end - start))
		return q 
	return wrapper


def coroutine(fn):
	"""
	decorator used to wrap all of the co-routine functions in this module,
	given that coroutines must be 'primed' by first calling 'next'
	"""
	def start(*args, **kwargs):
		g = fn(*args, **kwargs)
		next(g)
		return g
	return start


fname = "/Users/dougybarbo/Downloads/50k_sku_rows.csv"
fh = open(fname, 'r', encoding='utf-8')

fname_out = "/Users/dougybarbo/Documents/pipe.txt"



def opener(file_handle, target):
	'''
	source: only .send (and yield as entry point)
	'''
	reader = CSV.reader(file_handle, delimiter='\t', lineterminator='\r\n', quotechar='|')
	for line in reader:
		newline = line[2:-3]
		newline.append(line[-1])
		target.send(newline)
		


#------------------------------------- begin two-way workers -----------------------------------#
	
@coroutine
def grep1(filter, target):
	'''
	a worker
	'''
	try:
		while 1:
			line = (yield)
			try:
				if filter(line):
					target.send(line)
			except ValueError:
				err.append(line)
	except GeneratorExit:
		target.close()

#------------------------------------- end two-way workers -----------------------------------#


@coroutine
def broadcast(targets):
	while 1:
		line = (yield)
		for target in targets:
			target.send(line)



@coroutine
def persist():
	'''
	sink: only yield (no send)
	'''
	fh = open(fname_out, 'w+', encoding='utf-8')
	while 1:
		line = (yield)
		print(line, file=fh)
	fh.close()
	

@timerfn
def main():
	pass


if __name__=='__main__':
	
	# cProfile.run("main()")
	# opener(fh, grep1(hiprice, grep1(isrug, persist())))
	
	# alternative:
	
	opener(fh, 
		broadcast(
			[ 	grep1(hiprice, persist()),
				grep1(isrug, persist())
			])
	)
	
	