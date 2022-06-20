"use strict";(self.webpackChunkdagger=self.webpackChunkdagger||[]).push([[6905],{3905:function(e,t,n){n.d(t,{Zo:function(){return u},kt:function(){return f}});var r=n(7294);function a(e,t,n){return t in e?Object.defineProperty(e,t,{value:n,enumerable:!0,configurable:!0,writable:!0}):e[t]=n,e}function i(e,t){var n=Object.keys(e);if(Object.getOwnPropertySymbols){var r=Object.getOwnPropertySymbols(e);t&&(r=r.filter((function(t){return Object.getOwnPropertyDescriptor(e,t).enumerable}))),n.push.apply(n,r)}return n}function o(e){for(var t=1;t<arguments.length;t++){var n=null!=arguments[t]?arguments[t]:{};t%2?i(Object(n),!0).forEach((function(t){a(e,t,n[t])})):Object.getOwnPropertyDescriptors?Object.defineProperties(e,Object.getOwnPropertyDescriptors(n)):i(Object(n)).forEach((function(t){Object.defineProperty(e,t,Object.getOwnPropertyDescriptor(n,t))}))}return e}function l(e,t){if(null==e)return{};var n,r,a=function(e,t){if(null==e)return{};var n,r,a={},i=Object.keys(e);for(r=0;r<i.length;r++)n=i[r],t.indexOf(n)>=0||(a[n]=e[n]);return a}(e,t);if(Object.getOwnPropertySymbols){var i=Object.getOwnPropertySymbols(e);for(r=0;r<i.length;r++)n=i[r],t.indexOf(n)>=0||Object.prototype.propertyIsEnumerable.call(e,n)&&(a[n]=e[n])}return a}var d=r.createContext({}),s=function(e){var t=r.useContext(d),n=t;return e&&(n="function"==typeof e?e(t):o(o({},t),e)),n},u=function(e){var t=s(e.components);return r.createElement(d.Provider,{value:t},e.children)},p={inlineCode:"code",wrapper:function(e){var t=e.children;return r.createElement(r.Fragment,{},t)}},c=r.forwardRef((function(e,t){var n=e.components,a=e.mdxType,i=e.originalType,d=e.parentName,u=l(e,["components","mdxType","originalType","parentName"]),c=s(n),f=a,h=c["".concat(d,".").concat(f)]||c[f]||p[f]||i;return n?r.createElement(h,o(o({ref:t},u),{},{components:n})):r.createElement(h,o({ref:t},u))}));function f(e,t){var n=arguments,a=t&&t.mdxType;if("string"==typeof e||a){var i=n.length,o=new Array(i);o[0]=c;var l={};for(var d in t)hasOwnProperty.call(t,d)&&(l[d]=t[d]);l.originalType=e,l.mdxType="string"==typeof e?e:a,o[1]=l;for(var s=2;s<i;s++)o[s]=n[s];return r.createElement.apply(null,o)}return r.createElement.apply(null,n)}c.displayName="MDXCreateElement"},4585:function(e,t,n){n.r(t),n.d(t,{contentTitle:function(){return d},default:function(){return c},frontMatter:function(){return l},metadata:function(){return s},toc:function(){return u}});var r=n(7462),a=n(3366),i=(n(7294),n(3905)),o=["components"],l={},d=void 0,s={unversionedId:"rfcs/python_udf",id:"rfcs/python_udf",isDocsHomePage:!1,title:"python_udf",description:"Motivation",source:"@site/docs/rfcs/20220504_python_udf.md",sourceDirName:"rfcs",slug:"/rfcs/python_udf",permalink:"/dagger/docs/rfcs/python_udf",editUrl:"https://github.com/odpf/dagger/edit/master/docs/docs/rfcs/20220504_python_udf.md",tags:[],version:"current",sidebarPosition:20220504,frontMatter:{}},u=[{value:"Motivation",id:"motivation",children:[]},{value:"Requirement",id:"requirement",children:[]},{value:"Python User Defined Function",id:"python-user-defined-function",children:[]},{value:"Configuration",id:"configuration",children:[]},{value:"Registering the Udf",id:"registering-the-udf",children:[]},{value:"Release the Udf",id:"release-the-udf",children:[]},{value:"Reference",id:"reference",children:[]}],p={toc:u};function c(e){var t=e.components,n=(0,a.Z)(e,o);return(0,i.kt)("wrapper",(0,r.Z)({},p,n,{components:t,mdxType:"MDXLayout"}),(0,i.kt)("h2",{id:"motivation"},"Motivation"),(0,i.kt)("p",null,"Dagger users include developers, analysts, data scientists, etc. For users to use Dagger, they can add new capabilities by defining their own functions commonly referred to as UDFs. Currently, Dagger only supports java as the language for the UDFs. To democratize the process of creating and maintaining the UDFs we want to add support for python."),(0,i.kt)("h2",{id:"requirement"},"Requirement"),(0,i.kt)("p",null,"Support for adding Python UDF on Dagger\nEnd-to-end flow on adding and using Python UDF on Dagger. "),(0,i.kt)("h2",{id:"python-user-defined-function"},"Python User Defined Function"),(0,i.kt)("p",null,"There are two kinds of Python UDF that can be registered on Dagger:"),(0,i.kt)("ul",null,(0,i.kt)("li",{parentName:"ul"},"General Python UDF"),(0,i.kt)("li",{parentName:"ul"},"Vectorized Python UDF")),(0,i.kt)("p",null,'It shares a similar way as the general user-defined functions on how to define vectorized user-defined functions. Users only need to add an extra parameter func_type="pandas" in the decorator udf or udaf to mark it as a vectorized user-defined function.'),(0,i.kt)("table",null,(0,i.kt)("thead",{parentName:"table"},(0,i.kt)("tr",{parentName:"thead"},(0,i.kt)("th",{parentName:"tr",align:null},"Type"),(0,i.kt)("th",{parentName:"tr",align:null},"General Python UDF"),(0,i.kt)("th",{parentName:"tr",align:null},"Vectorized Python UDF"))),(0,i.kt)("tbody",{parentName:"table"},(0,i.kt)("tr",{parentName:"tbody"},(0,i.kt)("td",{parentName:"tr",align:null},"Data Processing Method"),(0,i.kt)("td",{parentName:"tr",align:null},"One piece of data is processed each time a UDF is called"),(0,i.kt)("td",{parentName:"tr",align:null},"Multiple pieces of data are processed each time a UDF is called")),(0,i.kt)("tr",{parentName:"tbody"},(0,i.kt)("td",{parentName:"tr",align:null},"Serialization/Deserialization"),(0,i.kt)("td",{parentName:"tr",align:null},"Serialization and Deserialization are required for each piece of data on the Java side and Python side"),(0,i.kt)("td",{parentName:"tr",align:null},"The data transmission format between Java and Python is based on Apache Arrow: ",(0,i.kt)("ul",null,(0,i.kt)("li",null," Pandas supports Apache Arrow natively, so serialization and deserialization are not required on Python side"),(0,i.kt)("li",null,"On the Java side, vectorized optimization is possible, and serialization/deserialization can be avoided as much as possible")))),(0,i.kt)("tr",{parentName:"tbody"},(0,i.kt)("td",{parentName:"tr",align:null},"Exection Performance"),(0,i.kt)("td",{parentName:"tr",align:null},"Poor"),(0,i.kt)("td",{parentName:"tr",align:null},"Good",(0,i.kt)("ul",null,(0,i.kt)("li",null,"Vectorized execution is of high efficiency"),(0,i.kt)("li",null,"High-performance python UDF can be implemented based on high performance libraries such as pandas and numpy")))))),(0,i.kt)("p",null,"Note: "),(0,i.kt)("p",null,"When using vectorized udf, Flink will convert the messages to pandas.series, and the udf will use that as an input and the output also pandas.series. The pandas.series size for input and output should be the same."),(0,i.kt)("h2",{id:"configuration"},"Configuration"),(0,i.kt)("p",null,"There are a few configurations that required for using python UDF, and also options we can adjust for optimization."),(0,i.kt)("p",null,'Configuration that will be added on Dagger codebase:\n| Key | Default | Type | Example\n| --- | ---     | ---  | ----  |\n|PYTHON_UDF_ENABLE|false|Boolean|false|\n|PYTHON_UDF_CONFIG|(none)|String|{"PYTHON_FILES":"/path/to/files.zip", "PYTHON_REQUIREMENTS": "requirements.txt", "PYTHON_FN_EXECUTION_BUNDLE_SIZE": "1000"}|'),(0,i.kt)("p",null,"The following variables than can be configurable on ",(0,i.kt)("inlineCode",{parentName:"p"},"PYTHON_UDF_CONFIG"),":\n| Key | Default | Type | Example\n| --- | ---     | ---  | ----  |\n|PYTHON_ARCHIVES|(none)|String|/path/to/data.zip|\n|PYTHON_FILES|(none)|String|/path/to/files.zip|\n|PYTHON_REQUIREMENTS|(none)|String|/path/to/requirements.txt|\n|PYTHON_FN_EXECUTION_ARROW_BATCH_SIZE|10000|Integer|10000|\n|PYTHON_FN_EXECUTION_BUNDLE_SIZE|100000|Integer|100000|\n|PYTHON_FN_EXECUTION_BUNDLE_TIME|1000|Long|1000|"),(0,i.kt)("h2",{id:"registering-the-udf"},"Registering the Udf"),(0,i.kt)("p",null,"Dagger will automatically register the python udf as long as the files meets the following criteria:"),(0,i.kt)("ul",null,(0,i.kt)("li",{parentName:"ul"},(0,i.kt)("p",{parentName:"li"},"Python file names should be the same with its function method\nExample:"),(0,i.kt)("p",{parentName:"li"},"sample.py"),(0,i.kt)("pre",{parentName:"li"},(0,i.kt)("code",{parentName:"pre"},'from pyflink.table import DataTypes\nfrom pyflink.table.udf import udf\n\n\n@udf(result_type=DataTypes.STRING())\ndef sample(word: str):\n    return word + "_test"\n'))),(0,i.kt)("li",{parentName:"ul"},(0,i.kt)("p",{parentName:"li"},"Avoid adding duplicate ",(0,i.kt)("inlineCode",{parentName:"p"},".py")," filenames. e.g: ",(0,i.kt)("inlineCode",{parentName:"p"},"__init__.py")))),(0,i.kt)("h2",{id:"release-the-udf"},"Release the Udf"),(0,i.kt)("p",null,"List of udfs for dagger, will be added on directory ",(0,i.kt)("inlineCode",{parentName:"p"},"dagger-py-functions")," include with its test, data files that are used on the udf, and the udf dependency(requirements.txt).\nAll of these files will be bundled to single zip file and uploaded to assets on release."),(0,i.kt)("h2",{id:"reference"},"Reference"),(0,i.kt)("p",null,(0,i.kt)("a",{parentName:"p",href:"https://nightlies.apache.org/flink/flink-docs-release-1.14/docs/dev/python/table/udfs/overview/"},"Flink Python User Defined Functions")))}c.isMDXComponent=!0}}]);