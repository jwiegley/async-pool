{ compiler    ? "ghc92"
, rev         ? "14ccaaedd95a488dd7ae142757884d8e125b3363"
, sha256      ? "1dvdpwdzkzr9pkvb7pby0aajgx7qv34qaxb1bjxx4dxi3aip9q5q"
, doBenchmark ? false
, doTracing   ? false
, doStrict    ? false
, pkgs        ? import (builtins.fetchTarball {
    url = "https://github.com/NixOS/nixpkgs/archive/${rev}.tar.gz";
    inherit sha256; }) {
    config.allowUnfree = true;
    config.allowBroken = false;
  }
, returnShellEnv ? pkgs.lib.inNixShell
, mkDerivation ? null
}:

let haskellPackages = pkgs.haskell.packages.${compiler};

in haskellPackages.developPackage {
  root = ./.;

  source-overrides = {
  };

  modifier = drv: pkgs.haskell.lib.overrideCabal drv (attrs: {
    inherit doBenchmark;
  });

  inherit returnShellEnv;
}
