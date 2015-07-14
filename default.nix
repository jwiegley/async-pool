{ mkDerivation, async, base, containers, fgl, hspec, monad-control
, stdenv, stm, time, transformers, transformers-base
}:
mkDerivation {
  pname = "async-pool";
  version = "0.9.0";
  src = ./.;
  buildDepends = [
    async base containers fgl monad-control stm transformers
    transformers-base
  ];
  testDepends = [
    async base containers fgl hspec monad-control stm time transformers
    transformers-base
  ];
  description = "A modified version of async that supports worker groups and many-to-many task dependencies";
  license = stdenv.lib.licenses.mit;
}
