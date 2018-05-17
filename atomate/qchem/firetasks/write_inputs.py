# coding: utf-8

from __future__ import division, print_function, unicode_literals, absolute_import

# This module defines firetasks for writing QChem input files

from atomate.utils.utils import load_class
from fireworks import FiretaskBase, explicit_serialize
from pymatgen.core import Molecule

__author__ = 'Brandon Wood'
__email__ = "b.wood@berkeley.edu"


@explicit_serialize
class WriteInputFromIOSet(FiretaskBase):
    """
    Writes QChem Input files from input sets. A dictionary is passed to WriteInputFromIOSet where
    parameters are given as keys in the dictionary.

    required_params:
        qc_input_set (QChemDictSet or str): Either a QChemDictSet object or a string
        name for the QChem input set (e.g., "OptSet"). *** Note that if the molecule is to be inherited through
        fw_spec qc_input_set must be a string name for the QChem input set. ***

    optional_params:
        qchem_input_params (dict): When using a string name for QChem input set, use this as a dict
        to specify kwargs for instantiating the input set parameters. For example, if you want
        to change the DFT_rung, you should provide: {"DFT_rung": ...}.
        This setting is ignored if you provide the full object representation of a QChemDictSet
        rather than a String.
        molecule (Molecule):
        input_file (str): Name of the QChem input file. Defaults to 
    """

    required_params = ["qchem_input_set"]
    optional_params = ["molecule", "qchem_input_params", "input_file"]

    def run_task(self, fw_spec):
        input_file = "mol.qin"
        if "input_file" in self:
            input_file = self["input_file"]
        # these if statements might need to be reordered at some point
        # if a full QChemDictSet object was provided
        if hasattr(self["qchem_input_set"], "write_file"):
            qcin = self["qchem_input_set"]
            qcin.write_file(input_file)
        # if a molecule is being passed through fw_spec
        elif fw_spec.get("prev_calc_molecule"):
            mol = Molecule.from_dict(fw_spec.get("prev_calc_molecule"))
            qcin_cls = load_class("pymatgen.io.qchem_io.sets", self["qchem_input_set"])
            qcin = qcin_cls(mol, **self.get("qchem_input_params", {}))
            qcin.write_file(input_file)
        # if a molecule is included as an optional parameter
        elif self.get("molecule"):
            qcin_cls = load_class("pymatgen.io.qchem_io.sets", self["qchem_input_set"])
            qcin = qcin_cls(self.get("molecule"), **self.get("qchem_input_params", {}))
            qcin.write_file(input_file)
        # if no molecule is present raise an error
        else:
            raise KeyError("No molecule present, add as an optional param or check fw_spec")

@explicit_serialize
class WriteInput(FiretaskBase):
    """
    Writes QChem input file from QCInput object.

    required_params:
        qc_input (QCInput): QCInput object

    """
    required_params = ["qc_input"]

    def run_task(self, fw_spec):
        # if a QCInput object is provided
        qcin = self['qc_input']
        qcin.write_file("mol.qin")
