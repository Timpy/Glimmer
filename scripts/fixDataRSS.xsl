<?xml version="1.0"?>

<xsl:stylesheet xmlns:xsl="http://www.w3.org/1999/XSL/Transform" version="1.0">

    <xsl:output method="xml"/>
    <!--
   Stylesheet to add default namespaces to dataRSS and fix RDFa extraction issues.
   
   Note: the use of xml:namespace requires XSLT 2.0 compatible engine
    -->
    
    <!-- Normal processing step: match the root element and add the default namespaces -->
    <xsl:template match="/feed">

    <!-- Add default namespaces: requires XSLT version 2.0
        <xsl:variable name="table_namespaces"
            select="document('t_namespaces.html')/table/tr/td[string-length(.)&gt;0][1]"/>


        <xsl:element name="feed">
            <xsl:for-each select="$table_namespaces">

                <xsl:namespace name="{.}" select="following-sibling::td[3]"/>
           
                 
        
            </xsl:for-each>

            <xsl:apply-templates select="*"/>

        </xsl:element>
     -->
             <feed>
                <xsl:apply-templates select="*"/>
             </feed>
    </xsl:template>

    <!-- Bugfix: type elements with an enclosing item that has a resource attribute, repeat the resource attribute -->
    <xsl:template match="type">
        <xsl:element name="type">
            <xsl:if test="@typeof">
                <xsl:attribute name="typeof">
                    <xsl:value-of select="@typeof"/>
                </xsl:attribute>
            </xsl:if>
            <xsl:if test="parent::item[@resource]">
                <xsl:attribute name="resource">
                    <xsl:value-of select="parent::item/@resource"/>
                </xsl:attribute>
            </xsl:if>
	    <xsl:apply-templates select="*"/>
        </xsl:element>
    </xsl:template>

    <!-- Identity transform -->
    <xsl:template match="@*|node()">
        <xsl:copy>
            <xsl:apply-templates select="@*|node()"/>
        </xsl:copy>
    </xsl:template>

</xsl:stylesheet>
