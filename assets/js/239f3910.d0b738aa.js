"use strict";(self.webpackChunkoptimus=self.webpackChunkoptimus||[]).push([[9887],{3905:(e,t,n)=>{n.d(t,{Zo:()=>l,kt:()=>d});var o=n(7294);function r(e,t,n){return t in e?Object.defineProperty(e,t,{value:n,enumerable:!0,configurable:!0,writable:!0}):e[t]=n,e}function i(e,t){var n=Object.keys(e);if(Object.getOwnPropertySymbols){var o=Object.getOwnPropertySymbols(e);t&&(o=o.filter((function(t){return Object.getOwnPropertyDescriptor(e,t).enumerable}))),n.push.apply(n,o)}return n}function a(e){for(var t=1;t<arguments.length;t++){var n=null!=arguments[t]?arguments[t]:{};t%2?i(Object(n),!0).forEach((function(t){r(e,t,n[t])})):Object.getOwnPropertyDescriptors?Object.defineProperties(e,Object.getOwnPropertyDescriptors(n)):i(Object(n)).forEach((function(t){Object.defineProperty(e,t,Object.getOwnPropertyDescriptor(n,t))}))}return e}function c(e,t){if(null==e)return{};var n,o,r=function(e,t){if(null==e)return{};var n,o,r={},i=Object.keys(e);for(o=0;o<i.length;o++)n=i[o],t.indexOf(n)>=0||(r[n]=e[n]);return r}(e,t);if(Object.getOwnPropertySymbols){var i=Object.getOwnPropertySymbols(e);for(o=0;o<i.length;o++)n=i[o],t.indexOf(n)>=0||Object.prototype.propertyIsEnumerable.call(e,n)&&(r[n]=e[n])}return r}var s=o.createContext({}),u=function(e){var t=o.useContext(s),n=t;return e&&(n="function"==typeof e?e(t):a(a({},t),e)),n},l=function(e){var t=u(e.components);return o.createElement(s.Provider,{value:t},e.children)},p="mdxType",m={inlineCode:"code",wrapper:function(e){var t=e.children;return o.createElement(o.Fragment,{},t)}},f=o.forwardRef((function(e,t){var n=e.components,r=e.mdxType,i=e.originalType,s=e.parentName,l=c(e,["components","mdxType","originalType","parentName"]),p=u(n),f=r,d=p["".concat(s,".").concat(f)]||p[f]||m[f]||i;return n?o.createElement(d,a(a({ref:t},l),{},{components:n})):o.createElement(d,a({ref:t},l))}));function d(e,t){var n=arguments,r=t&&t.mdxType;if("string"==typeof e||r){var i=n.length,a=new Array(i);a[0]=f;var c={};for(var s in t)hasOwnProperty.call(t,s)&&(c[s]=t[s]);c.originalType=e,c[p]="string"==typeof e?e:r,a[1]=c;for(var u=2;u<i;u++)a[u]=n[u];return o.createElement.apply(null,a)}return o.createElement.apply(null,n)}f.displayName="MDXCreateElement"},3513:(e,t,n)=>{n.r(t),n.d(t,{assets:()=>s,contentTitle:()=>a,default:()=>m,frontMatter:()=>i,metadata:()=>c,toc:()=>u});var o=n(7462),r=(n(7294),n(3905));const i={},a="Contributing",c={unversionedId:"contribute/contribution-process",id:"contribute/contribution-process",title:"Contributing",description:"First off, thanks for taking the time to contribute! \ud83c\udf1f\ud83e\udd73",source:"@site/docs/contribute/contribution-process.md",sourceDirName:"contribute",slug:"/contribute/contribution-process",permalink:"/optimus/docs/contribute/contribution-process",draft:!1,editUrl:"https://github.com/goto/optimus/edit/master/docs/docs/contribute/contribution-process.md",tags:[],version:"current",lastUpdatedBy:"Oky Setiawan",lastUpdatedAt:1718956122,formattedLastUpdatedAt:"Jun 21, 2024",frontMatter:{},sidebar:"docsSidebar",previous:{title:"Tutorial of Plugin Development",permalink:"/optimus/docs/building-plugin/tutorial"},next:{title:"Developer Environment Setup",permalink:"/optimus/docs/contribute/developer-env-setup"}},s={},u=[{value:"Best practices",id:"best-practices",level:2},{value:"Code of Conduct",id:"code-of-conduct",level:2}],l={toc:u},p="wrapper";function m(e){let{components:t,...n}=e;return(0,r.kt)(p,(0,o.Z)({},l,n,{components:t,mdxType:"MDXLayout"}),(0,r.kt)("h1",{id:"contributing"},"Contributing"),(0,r.kt)("p",null,"First off, thanks for taking the time to contribute! \ud83c\udf1f\ud83e\udd73"),(0,r.kt)("p",null,"Before start contributing, feel free to ask questions or initiate conversation via GitHub discussion.\nYou are also welcome to create issue if you encounter a bug or to suggest feature enhancements."),(0,r.kt)("p",null,"Please note we have a code of conduct, please follow it in all your interactions with the project."),(0,r.kt)("h2",{id:"best-practices"},"Best practices"),(0,r.kt)("ul",null,(0,r.kt)("li",{parentName:"ul"},"Follow the ",(0,r.kt)("a",{parentName:"li",href:"https://www.conventionalcommits.org/en/v1.0.0/"},"conventional commit")," format for all commit messages."),(0,r.kt)("li",{parentName:"ul"},"Link the PR with the issue. This is mandatory to ensure there is sufficient information for the reviewer to understand\nyour PR."),(0,r.kt)("li",{parentName:"ul"},"When you make a PR for small change (such as fixing a typo, style change, or grammar fix), please squash your commits\nso that we can maintain a cleaner git history."),(0,r.kt)("li",{parentName:"ul"},"Docs live in the code repo under ",(0,r.kt)("a",{parentName:"li",href:"https://github.com/goto/optimus/tree/main/docs"},"docs"),". Please maintain the docs\nand any docs changes can be done in the same PR."),(0,r.kt)("li",{parentName:"ul"},"Avoid force-pushing as it makes reviewing difficult.")),(0,r.kt)("h2",{id:"code-of-conduct"},"Code of Conduct"),(0,r.kt)("p",null,"Examples of behavior that contributes to creating a positive environment include:"),(0,r.kt)("ul",null,(0,r.kt)("li",{parentName:"ul"},"Using welcoming and inclusive language"),(0,r.kt)("li",{parentName:"ul"},"Being respectful of differing viewpoints and experiences"),(0,r.kt)("li",{parentName:"ul"},"Gracefully accepting constructive criticism"),(0,r.kt)("li",{parentName:"ul"},"Focusing on what is best for the project")),(0,r.kt)("p",null,"Things to keep in mind before creating a new commit:"),(0,r.kt)("ul",null,(0,r.kt)("li",{parentName:"ul"},"Go through the project code conventions."),(0,r.kt)("li",{parentName:"ul"},"Commit ",(0,r.kt)("a",{parentName:"li",href:"https://www.conventionalcommits.org/en/v1.0.0/"},"guidelines")),(0,r.kt)("li",{parentName:"ul"},(0,r.kt)("a",{parentName:"li",href:"https://github.com/cncf/foundation/blob/master/code-of-conduct.md"},"CNCF Code of Conduct"))))}m.isMDXComponent=!0}}]);