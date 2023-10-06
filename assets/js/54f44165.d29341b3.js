"use strict";(self.webpackChunkoptimus=self.webpackChunkoptimus||[]).push([[152],{3905:(e,t,n)=>{n.d(t,{Zo:()=>p,kt:()=>d});var a=n(7294);function r(e,t,n){return t in e?Object.defineProperty(e,t,{value:n,enumerable:!0,configurable:!0,writable:!0}):e[t]=n,e}function l(e,t){var n=Object.keys(e);if(Object.getOwnPropertySymbols){var a=Object.getOwnPropertySymbols(e);t&&(a=a.filter((function(t){return Object.getOwnPropertyDescriptor(e,t).enumerable}))),n.push.apply(n,a)}return n}function i(e){for(var t=1;t<arguments.length;t++){var n=null!=arguments[t]?arguments[t]:{};t%2?l(Object(n),!0).forEach((function(t){r(e,t,n[t])})):Object.getOwnPropertyDescriptors?Object.defineProperties(e,Object.getOwnPropertyDescriptors(n)):l(Object(n)).forEach((function(t){Object.defineProperty(e,t,Object.getOwnPropertyDescriptor(n,t))}))}return e}function o(e,t){if(null==e)return{};var n,a,r=function(e,t){if(null==e)return{};var n,a,r={},l=Object.keys(e);for(a=0;a<l.length;a++)n=l[a],t.indexOf(n)>=0||(r[n]=e[n]);return r}(e,t);if(Object.getOwnPropertySymbols){var l=Object.getOwnPropertySymbols(e);for(a=0;a<l.length;a++)n=l[a],t.indexOf(n)>=0||Object.prototype.propertyIsEnumerable.call(e,n)&&(r[n]=e[n])}return r}var s=a.createContext({}),u=function(e){var t=a.useContext(s),n=t;return e&&(n="function"==typeof e?e(t):i(i({},t),e)),n},p=function(e){var t=u(e.components);return a.createElement(s.Provider,{value:t},e.children)},c="mdxType",g={inlineCode:"code",wrapper:function(e){var t=e.children;return a.createElement(a.Fragment,{},t)}},m=a.forwardRef((function(e,t){var n=e.components,r=e.mdxType,l=e.originalType,s=e.parentName,p=o(e,["components","mdxType","originalType","parentName"]),c=u(n),m=r,d=c["".concat(s,".").concat(m)]||c[m]||g[m]||l;return n?a.createElement(d,i(i({ref:t},p),{},{components:n})):a.createElement(d,i({ref:t},p))}));function d(e,t){var n=arguments,r=t&&t.mdxType;if("string"==typeof e||r){var l=n.length,i=new Array(l);i[0]=m;var o={};for(var s in t)hasOwnProperty.call(t,s)&&(o[s]=t[s]);o.originalType=e,o[c]="string"==typeof e?e:r,i[1]=o;for(var u=2;u<l;u++)i[u]=n[u];return a.createElement.apply(null,i)}return a.createElement.apply(null,n)}m.displayName="MDXCreateElement"},681:(e,t,n)=>{n.r(t),n.d(t,{assets:()=>s,contentTitle:()=>i,default:()=>g,frontMatter:()=>l,metadata:()=>o,toc:()=>u});var a=n(7462),r=(n(7294),n(3905));const l={},i="Installation",o={unversionedId:"getting-started/installation",id:"getting-started/installation",title:"Installation",description:"Installing Optimus on any system is straight forward. There are several approaches to install Optimus:",source:"@site/docs/getting-started/installation.md",sourceDirName:"getting-started",slug:"/getting-started/installation",permalink:"/optimus/docs/getting-started/installation",draft:!1,editUrl:"https://github.com/goto/optimus/edit/master/docs/docs/getting-started/installation.md",tags:[],version:"current",lastUpdatedBy:"Dery Rahman Ahaddienata",lastUpdatedAt:1696574068,formattedLastUpdatedAt:"Oct 6, 2023",frontMatter:{},sidebar:"docsSidebar",previous:{title:"Introduction",permalink:"/optimus/docs/introduction"},next:{title:"Quickstart",permalink:"/optimus/docs/getting-started/quick-start"}},s={},u=[{value:"Using a Pre-built Binary",id:"using-a-pre-built-binary",level:2},{value:"Installing with Package Manager",id:"installing-with-package-manager",level:2},{value:"Installing using Docker",id:"installing-using-docker",level:2},{value:"Installing from Source",id:"installing-from-source",level:2},{value:"Prerequisites",id:"prerequisites",level:3},{value:"Build",id:"build",level:3}],p={toc:u},c="wrapper";function g(e){let{components:t,...n}=e;return(0,r.kt)(c,(0,a.Z)({},p,n,{components:t,mdxType:"MDXLayout"}),(0,r.kt)("h1",{id:"installation"},"Installation"),(0,r.kt)("p",null,"Installing Optimus on any system is straight forward. There are several approaches to install Optimus:"),(0,r.kt)("ul",null,(0,r.kt)("li",{parentName:"ul"},"Using a pre-built binary"),(0,r.kt)("li",{parentName:"ul"},"Installing with package manager"),(0,r.kt)("li",{parentName:"ul"},"Installing with Docker"),(0,r.kt)("li",{parentName:"ul"},"Installing from source")),(0,r.kt)("h2",{id:"using-a-pre-built-binary"},"Using a Pre-built Binary"),(0,r.kt)("p",null,"The client and server binaries are downloadable at the ",(0,r.kt)("a",{parentName:"p",href:"https://github.com/goto/optimus/releases"},"releases")," section."),(0,r.kt)("p",null,"Once installed, you should be able to run:"),(0,r.kt)("pre",null,(0,r.kt)("code",{parentName:"pre",className:"language-shell"},"$ optimus version\n")),(0,r.kt)("h2",{id:"installing-with-package-manager"},"Installing with Package Manager"),(0,r.kt)("p",null,"For macOS, you can install Optimus using homebrew:"),(0,r.kt)("pre",null,(0,r.kt)("code",{parentName:"pre",className:"language-shell"},"$ brew install goto/tap/optimus\n$ optimus version\n")),(0,r.kt)("h2",{id:"installing-using-docker"},"Installing using Docker"),(0,r.kt)("p",null,"To pull latest image:"),(0,r.kt)("pre",null,(0,r.kt)("code",{parentName:"pre",className:"language-shell"},"$ docker pull goto/optimus:latest\n")),(0,r.kt)("p",null,"To pull specific image:"),(0,r.kt)("pre",null,(0,r.kt)("code",{parentName:"pre",className:"language-shell"},"$ docker pull goto/optimus:0.6.0\n")),(0,r.kt)("h2",{id:"installing-from-source"},"Installing from Source"),(0,r.kt)("h3",{id:"prerequisites"},"Prerequisites"),(0,r.kt)("p",null,"Optimus requires the following dependencies:"),(0,r.kt)("ul",null,(0,r.kt)("li",{parentName:"ul"},"Golang (version 1.18 or above)"),(0,r.kt)("li",{parentName:"ul"},"Git")),(0,r.kt)("h3",{id:"build"},"Build"),(0,r.kt)("p",null,"Run the following commands to compile optimus from source"),(0,r.kt)("pre",null,(0,r.kt)("code",{parentName:"pre",className:"language-shell"},"$ git clone git@github.com:goto/optimus.git\n$ cd optimus\n$ make build\n")),(0,r.kt)("p",null,"Use the following command to test"),(0,r.kt)("pre",null,(0,r.kt)("code",{parentName:"pre",className:"language-shell"},"$ optimus version\n")))}g.isMDXComponent=!0}}]);