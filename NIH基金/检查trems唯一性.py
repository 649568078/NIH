import re

a1 = "Address;Animals;Astrocytes;Axon;Biochemical;Bioenergetics;Cell Survival;Cell physiology;Cells;Chronic;Complex;Coupling;Data;Dependence;Detection;Development;Disease;Down-Regulation;Elements;Equilibrium;Etiology;Functional disorder;Gene Expression;Genes;Glaucoma;Glycolysis;Goals;Homeostasis;Human;Hypoxia;Incidence;Injury;Intervention;Investigation;Knockout Mice;Maintenance;Mediating;Metabolic;Metabolic dysfunction;Metabolism;Mitochondria;Modeling;Muller&apos;s cell;Nerve Degeneration;Neuroglia;Neurons;Ocular Hypertension;Optic Disk;Optic Nerve;Oxidative Phosphorylation;Oxygen;Pathology;Perfusion;Physiologic Intraocular Pressure;Physiological;Positioning Attribute;Public Health;Recycling;Regulation;Research;Retina;Retinal Ganglion Cells;Stress;Testing;Therapeutic Intervention;Up-Regulation;Vision;Work;age related neurodegeneration;axonal degeneration;base;cell type;conditional knockout;design;functional decline;high intraocular pressure;human tissue;in vivo;innovation;insight;metabolic profile;metabolomics;mitochondrial dysfunction;mouse model;new therapeutic target;novel therapeutics;prevent;protein expression;relating to nervous system;resilience;retinal ganglion cell degeneration;stressor;transcriptomics"
a2 = "Address; age related neurodegeneration; Animals; Astrocytes; Axon; axonal degeneration; base; Biochemical; Bioenergetics; Cell physiology; Cell Survival; cell type; Cells; Chronic; Complex; conditional knockout; Coupling; Data; Dependence; design; Detection; Development; Disease; Down-Regulation; Elements; Equilibrium; Etiology; functional decline; Functional disorder; Gene Expression; Genes; Glaucoma; Glycolysis; Goals; high intraocular pressure; Homeostasis; Human; human tissue; Hypoxia; in vivo; Incidence; Injury; innovation; insight; Intervention; Investigation; Knockout Mice; Maintenance; Mediating; Metabolic; Metabolic dysfunction; metabolic profile; Metabolism; metabolomics; Mitochondria; mitochondrial dysfunction; Modeling; mouse model; Muller's cell; Nerve Degeneration; Neuroglia; Neurons; new therapeutic target; novel therapeutics; Ocular Hypertension; Optic Disk; Optic Nerve; Oxidative Phosphorylation; Oxygen; Pathology; Perfusion; Physiologic Intraocular Pressure; Physiological; Positioning Attribute; prevent; protein expression; Public Health; Recycling; Regulation; relating to nervous system; Research; resilience; Retina; retinal ganglion cell degeneration; Retinal Ganglion Cells; Stress; stressor; Testing; Therapeutic Intervention; transcriptomics; Up-Regulation; Vision; Work; "

list_a1 = re.split(r';', a1)
list_a2 = re.split(r';', a2)

list_a2_replace = []
for i in list_a2:
    i = re.sub("^ ","",i)
    #print(i)
    list_a2_replace.append(i)



print(list_a1)
print(list_a2_replace)


# a = [x for x in list_a1 if x in list_a2] #两个列表表都存在
# b = [y for y in (list_a1 + list_a1) if y not in a] #两个列表中的不同元素
#
# print(a)
# print(b)

c = [x for x in list_a1 if x in list_a2_replace] #两个列表表都存在
d = [y for y in (list_a1 + list_a2_replace) if y not in list_a1] #两个列表中的不同元素

print(c)
print(d)