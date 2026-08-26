package dc

type DCFile = dcFile

type DCClass = dcClass

type DCField = field

type DCAtomicField = atomicField

type DCMolecularField = molecularField

type DCParameter = field

type DCPacker = dcPacker

func NewDCFile() *DCFile { return newDCFile() }

func NewDCPacker() *DCPacker { return newDCPacker() }

func DeleteDCPacker(p *DCPacker) {}
