#include "Simulator.h"


int main(int argc, char **argv) {

  wrench::Simulator *simulator = new wrench::Simulator();
  return simulator->main(argc, argv);
}